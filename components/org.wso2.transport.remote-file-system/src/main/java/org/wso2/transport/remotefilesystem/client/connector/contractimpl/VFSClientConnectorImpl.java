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
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.transport.remotefilesystem.Constants;
import org.wso2.transport.remotefilesystem.client.connector.contract.FtpAction;
import org.wso2.transport.remotefilesystem.client.connector.contract.VFSClientConnector;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.listener.RemoteFileSystemListener;
import org.wso2.transport.remotefilesystem.message.RemoteFileSystemMessage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Implementation for {@link VFSClientConnector} interface.
 */
public class VFSClientConnectorImpl implements VFSClientConnector {

    private static final Logger logger = LoggerFactory.getLogger(VFSClientConnectorImpl.class);

    private Map<String, String> connectorConfig;
    private RemoteFileSystemListener remoteFileSystemListener;
    private FileSystemOptions opts = new FileSystemOptions();

    public VFSClientConnectorImpl(Map<String, String> config, RemoteFileSystemListener listener)
            throws RemoteFileSystemConnectorException {
        this.connectorConfig = config;
        this.remoteFileSystemListener = listener;
        final String url = config.get(Constants.URI);
        if (url != null) {
            if (url.startsWith(Constants.SCHEME_FTP)) {
                setFtpSystemConfig(config);
            } else if (url.startsWith(Constants.SCHEME_SFTP)) {
                setSftpSystemConfig(config);
            }
        }
    }

    @Override
    public void send(RemoteFileSystemMessage message, FtpAction action) {
        String fileURI = connectorConfig.get(Constants.URI);
        InputStream inputStream;
        OutputStream outputStream = null;
        ByteBuffer byteBuffer;
        FileObject path = null;
        boolean pathClose = true;
        try {
            FileSystemManager fsManager = VFS.getManager();
            path = fsManager.resolveFile(fileURI, opts);
            switch (action) {
                case MKDIR:
                    if (path.exists()) {
                        throw new RemoteFileSystemConnectorException("Directory exists: " + path.getName().getURI());
                    }
                    path.createFolder();
                    break;
                case PUT:
                case APPEND:
                    if (!path.exists()) {
                        path.createFile();
                        path.refresh();
                    }
                    if (FtpAction.APPEND.equals(action)) {
                        outputStream = path.getContent().getOutputStream(true);
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
                    break;
                case DELETE:
                    if (path.exists()) {
                        int filesDeleted = path.delete(Selectors.SELECT_SELF);
                        if (logger.isDebugEnabled()) {
                            logger.debug(filesDeleted + " files successfully deleted");
                        }
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to delete file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case RMDIR:
                    if (path.exists()) {
                        int filesDeleted = path.delete(Selectors.SELECT_ALL);
                        if (logger.isDebugEnabled()) {
                            logger.debug(filesDeleted + " files successfully deleted");
                        }
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to delete directory: " + path.getName().getURI() + " not found");
                    }
                    break;
                case RENAME:
                    if (path.exists()) {
                        String destination = connectorConfig.get(Constants.DESTINATION);
                        try (FileObject newPath = fsManager.resolveFile(destination, opts);
                                FileObject parent = newPath.getParent()) {
                            if (parent != null) {
                                if (!parent.exists()) {
                                    parent.createFolder();
                                }
                                try (FileObject finalPath = parent.resolveFile(newPath.getName().getBaseName())) {
                                    if (!finalPath.exists()) {
                                        path.moveTo(finalPath);
                                    } else {
                                        throw new RemoteFileSystemConnectorException(
                                                "The file at " + newPath.getURL().toString()
                                                        + " already exists or it is a directory");
                                    }
                                }
                            }
                        }
                    } else {
                        throw new RemoteFileSystemConnectorException(
                                "Failed to rename file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case GET:
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
                case SIZE:
                    remoteFileSystemListener.onMessage(new RemoteFileSystemMessage(path.getContent().getSize()));
                    break;
                case LIST:
                    final FileObject[] pathObjects = path.getChildren();
                    String[] childrenNames = new String[pathObjects.length];
                    int i = 0;
                    for (FileObject child : pathObjects) {
                        childrenNames[i++] = child.getName().getPath();
                    }
                    RemoteFileSystemMessage children = new RemoteFileSystemMessage(childrenNames);
                    remoteFileSystemListener.onMessage(children);
                    break;
                case ISDIR:
                    remoteFileSystemListener.onMessage(new RemoteFileSystemMessage(path.isFolder()));
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

    private void setSftpSystemConfig(Map<String, String> config) throws RemoteFileSystemConnectorException {
        final SftpFileSystemConfigBuilder configBuilder = SftpFileSystemConfigBuilder.getInstance();
        if (config.get(Constants.USER_DIR_IS_ROOT) != null) {
            configBuilder.setUserDirIsRoot(opts, Boolean.parseBoolean(Constants.USER_DIR_IS_ROOT));
        }
        if (config.get(Constants.IDENTITY) != null) {
            try {
                configBuilder.setIdentityInfo(opts, new IdentityInfo(new File(config.get(Constants.IDENTITY))));
            } catch (FileSystemException e) {
                throw new RemoteFileSystemConnectorException(e.getMessage(), e);
            }
        }
        if (config.get(Constants.IDENTITY_PASS_PHRASE) != null) {
            try {
                configBuilder.setIdentityPassPhrase(opts, config.get(Constants.IDENTITY_PASS_PHRASE));
            } catch (FileSystemException e) {
                throw new RemoteFileSystemConnectorException(e.getMessage(), e);
            }
        }
        if (config.get(Constants.AVOID_PERMISSION_CHECK) != null) {
            configBuilder.setAvoidPermissionCheck(opts, config.get(Constants.AVOID_PERMISSION_CHECK));
        }
    }

    private void setFtpSystemConfig(Map<String, String> config) {
        final FtpFileSystemConfigBuilder configBuilder = FtpFileSystemConfigBuilder.getInstance();
        if (config.get(Constants.PASSIVE_MODE) != null) {
            configBuilder.setPassiveMode(opts, Boolean.parseBoolean(config.get(Constants.PASSIVE_MODE)));
        }
        if (config.get(Constants.USER_DIR_IS_ROOT) != null) {
            configBuilder.setUserDirIsRoot(opts, Boolean.parseBoolean(Constants.USER_DIR_IS_ROOT));
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
