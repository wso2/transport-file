/*
 * Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.transport.remotefilesystem.client.connector.contractimpl;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.transport.remotefilesystem.Constants;
import org.wso2.transport.remotefilesystem.client.connector.contract.VFSClientConnector;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.listener.RemoteFileSystemListener;
import org.wso2.transport.remotefilesystem.message.RemoteFileSystemMessage;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;

/**
 * Implementation for {@link VFSClientConnector} interface.
 */
public class VFSClientConnectorImpl implements VFSClientConnector {

    private static final Logger logger = LoggerFactory.getLogger(VFSClientConnectorImpl.class);

    private Map<String, String> connectorConfig;
    private RemoteFileSystemListener remoteFileSystemListener;
    private FileSystemOptions opts = new FileSystemOptions();

    public VFSClientConnectorImpl(Map<String, String> connectorConfig,
                                  RemoteFileSystemListener remoteFileSystemListener) {
        this.connectorConfig = connectorConfig;
        this.remoteFileSystemListener = remoteFileSystemListener;

        if (Constants.PROTOCOL_FTP.equals(connectorConfig.get(Constants.PROTOCOL))) {
            connectorConfig.forEach((property, value) -> {
                // TODO: Add support for other FTP related configurations
                if (Constants.FTP_PASSIVE_MODE.equals(property)) {
                    FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, Boolean.parseBoolean(value));
                }
            });
        }
    }

    @Override
    public void send(RemoteFileSystemMessage message) {
//        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);
        String fileURI = connectorConfig.get(Constants.URI);
        String action = connectorConfig.get(Constants.ACTION);
        FileType fileType;
        InputStream inputStream;
        OutputStream outputStream = null;
        ByteBuffer byteBuffer;
        FileObject path = null;
        boolean pathClose = true;
        try {
            FileSystemManager fsManager = VFS.getManager();
            path = fsManager.resolveFile(fileURI, opts);
            fileType = path.getType();
            switch (action.toLowerCase(Locale.ENGLISH)) {
                case Constants.CREATE:
                    boolean isFolder = Boolean.parseBoolean(connectorConfig.getOrDefault(Constants.CREATE_FOLDER,
                            "false"));
                    if (path.exists()) {
                        throw new RemoteFileSystemConnectorException("File already exists: " + path.getName().getURI());
                    }
                    if (isFolder) {
                        path.createFolder();
                    } else {
                        path.createFile();
                    }
                    break;
                case Constants.WRITE:
                    if (!path.exists()) {
                        path.createFile();
                        path.refresh();
                        fileType = path.getType();
                    }
                    if (fileType == FileType.FILE) {
                        if (connectorConfig.get(Constants.APPEND) != null) {
                            outputStream = path.getContent().getOutputStream(
                                    Boolean.parseBoolean(connectorConfig.get(Constants.APPEND)));
                        } else {
                            outputStream = path.getContent().getOutputStream();
                        }
                        inputStream = message.getInputStream();
                        byteBuffer = message.getBytes();
                        if (byteBuffer != null) {
                            outputStream.write(byteBuffer.array());
                        } else if (inputStream != null) {
                            int n;
                            byte[] buffer = new byte[16384];
                            while ((n = inputStream.read(buffer)) > -1) {
                                outputStream.write(buffer, 0, n);
                            }
                        }
                        outputStream.flush();
                    }
                    break;
                case Constants.DELETE:
                    if (path.exists()) {
                        int filesDeleted = path.delete(Selectors.SELECT_ALL);
                        if (logger.isDebugEnabled()) {
                            logger.debug(filesDeleted + " files successfully deleted");
                        }
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to delete file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.COPY:
                    if (path.exists()) {
                        String destination = connectorConfig.get(Constants.DESTINATION);
                        FileObject fileObject = fsManager.resolveFile(destination, opts);
                        fileObject.copyFrom(path, Selectors.SELECT_ALL);
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to copy file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.MOVE:
                    if (path.exists()) {
                        //TODO: Improve this to fix issue #331
                        String destination = connectorConfig.get(Constants.DESTINATION);
                        FileObject newPath = fsManager.resolveFile(destination, opts);
                        FileObject parent = newPath.getParent();
                        if (parent != null && !parent.exists()) {
                            parent.createFolder();
                        }
                        if (!newPath.exists()) {
                            path.moveTo(newPath);
                        } else {
                            throw new RemoteFileSystemConnectorException("The file at " + newPath.getURL().toString() +
                                    " already exists or it is a directory");
                        }
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to move file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.READ:
                    if (path.exists()) {
                        inputStream = path.getContent().getInputStream();
                        FileObjectInputStream objectInputStream = new FileObjectInputStream(inputStream, path);
                        RemoteFileSystemMessage fileContent = new RemoteFileSystemMessage(objectInputStream);
                        remoteFileSystemListener.onMessage(fileContent);
                        // We can't close the FileObject or InputStream at the end. This InputStream will pass to upper
                        // layer and stream need to close from there once the usage is done. Along with that need to
                        // close the FileObject. If we close the FileObject now then InputStream will not get any data.
                        pathClose = false;
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to read file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.EXISTS:
                    RemoteFileSystemMessage fileContent = new RemoteFileSystemMessage(Boolean.toString(path.exists()));
                    remoteFileSystemListener.onMessage(fileContent);
                    break;
                default:
                    break;
            }
            remoteFileSystemListener.done();
        } catch (RemoteFileSystemConnectorException | IOException e) {
            remoteFileSystemListener.onError(e);
        } finally {
            if (path != null && pathClose) {
                try {
                    path.close();
                } catch (FileSystemException e) {
                    //Do nothing.
                }
            }
            closeQuietly(outputStream);
        }
    }

    /**
     * Closes streams quietly
     *
     * @param closeable The stream that should be closed
     */
    private static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            // Do nothing.
        }
    }
}
