/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.transport.file.connector.sender;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * A Client Connector implementation for file systems using the Apache VFS library for file operations
 */
public class VFSClientConnector implements ClientConnector {
    private static final Logger logger = LoggerFactory.getLogger(VFSClientConnector.class);
    private FileSystemOptions opts = new FileSystemOptions();
    private CarbonMessageProcessor carbonMessageProcessor;
    private String scheme;
    private Map<String, Object> properties;

    @Override
    public Object init(CarbonMessage cMsg, CarbonCallback callback, Map<String, Object> properties)
            throws ClientConnectorException {
        this.properties = properties;
        setOptions(properties);
        return Boolean.TRUE;
    }

    private void setOptions(Map<String, Object> properties) throws ClientConnectorException {
        //TODO: Handle FS options configuration for other protocols as well
        scheme = null;
        if (properties.get(Constants.PROTOCOL) != null) {
            scheme = properties.get(Constants.PROTOCOL).toString();
        } else if (properties.get(Constants.PROTOCOL) == null && properties.get(Constants.FILE_URI) != null) {
            scheme = UriParser.extractScheme(properties.get(Constants.FILE_URI).toString());
        }
        if (Constants.PROTOCOL_FTP.equals(scheme)) {
            if (properties.get(Constants.FTP_PASSIVE_MODE) != null) {
                FtpFileSystemConfigBuilder.getInstance().setPassiveMode
                        (opts, Boolean.parseBoolean(properties.get(Constants.FTP_PASSIVE_MODE).toString()));
            } else {
                FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
            }
            if (properties.get(Constants.USER_DIR_IS_ROOT) != null) {
                FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot
                        (opts, Boolean.parseBoolean(properties.get(Constants.USER_DIR_IS_ROOT).toString()));
            } else {
                FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot
                        (opts, true);
            }
        } else if (Constants.PROTOCOL_SFTP.equals(scheme)) {
            if (properties.get(Constants.USER_DIR_IS_ROOT) != null) {
                SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot
                        (opts, Boolean.parseBoolean(properties.get(Constants.USER_DIR_IS_ROOT).toString()));
            } else {
                FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot
                        (opts, true);
            }
            if (properties.get(Constants.IDENTITY) != null) {
                try {
                    SftpFileSystemConfigBuilder.getInstance().setIdentityInfo
                            (opts, new IdentityInfo(new File(properties.get(Constants.IDENTITY).toString())));
                } catch (FileSystemException e) {
                    throw new ClientConnectorException(e.getMessage(), e);
                }
            }
            if (properties.get(Constants.IDENTITY_PASS_PHRASE) != null) {
                try {
                    SftpFileSystemConfigBuilder.getInstance().setIdentityPassPhrase
                            (opts, properties.get(Constants.IDENTITY_PASS_PHRASE).toString());
                } catch (FileSystemException e) {
                    throw new ClientConnectorException(e.getMessage(), e);
                }
            }
            if (properties.get(Constants.AVOID_PERMISSION_CHECK) != null) {
                SftpFileSystemConfigBuilder.getInstance().setAvoidPermissionCheck
                        (opts, properties.get(Constants.AVOID_PERMISSION_CHECK).toString());
            } else {
                SftpFileSystemConfigBuilder.getInstance().setAvoidPermissionCheck
                        (opts, "true");
            }
        }
    }

    @Override
    public boolean send(CarbonMessage carbonMessage, CarbonCallback carbonCallback)
            throws ClientConnectorException {
        return false;
    }

    @Override
    public boolean send(CarbonMessage carbonMessage, CarbonCallback carbonCallback, Map<String, String> map)
            throws ClientConnectorException {
        String fileURI = map.get(Constants.FILE_URI);
        String action = map.get(Constants.ACTION);
        long readWaitTimeout = 1000;
        if (null != map.get(Constants.FILE_READ_WAIT_TIMEOUT)) {
            readWaitTimeout = Long.parseLong(map.get(Constants.FILE_READ_WAIT_TIMEOUT));
        }
        FileType fileType;
        ByteBuffer byteBuffer;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        BufferedReader bufferedReader = null;
        try {
            FileSystemManager fsManager = VFS.getManager();
            if (scheme == null && properties != null) {
                properties.put(Constants.FILE_URI, fileURI);
                setOptions(this.properties);
            }
            FileObject path = fsManager.resolveFile(fileURI, opts);
            fileType = path.getType();
            switch (action.toLowerCase(Locale.ENGLISH)) {
                case Constants.CREATE:
                    boolean isFolder = Boolean.parseBoolean(map.getOrDefault("create-folder", "false"));
                    if (path.exists()) {
                        throw new ClientConnectorException("File already exists: " + path.getName().getURI());
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
                        if (carbonMessage instanceof BinaryCarbonMessage) {
                            BinaryCarbonMessage binaryCarbonMessage = (BinaryCarbonMessage) carbonMessage;
                            byteBuffer = binaryCarbonMessage.readBytes();
                        } else {
                            throw new ClientConnectorException("Carbon message received is not a BinaryCarbonMessage");
                        }
                        byte[] bytes = byteBuffer.array();
                        if (map.get(Constants.APPEND) != null) {
                            outputStream = path.getContent().getOutputStream(
                                    Boolean.parseBoolean(map.get(Constants.APPEND)));
                        } else {
                            outputStream = path.getContent().getOutputStream();
                        }
                        outputStream.write(bytes);
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
                        throw new ClientConnectorException(
                                "Failed to delete file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.COPY:
                    if (path.exists()) {
                        String destination = map.get("destination");
                        FileObject dest = fsManager.resolveFile(destination, opts);
                        dest.copyFrom(path, Selectors.SELECT_ALL);
                    } else {
                        throw new ClientConnectorException(
                                "Failed to copy file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.MOVE:
                    if (path.exists()) {
                        String moveIfExistMode = map.get(Constants.MOVE_IF_EXIST_MODE);
                        //TODO: Improve this to fix issue #331
                        String destination = map.get("destination");
                        FileObject newPath = fsManager.resolveFile(destination, opts);
                        FileObject parent = newPath.getParent();
                        if (parent != null && !parent.exists()) {
                            parent.createFolder();
                        }
                        if (!newPath.exists()) {
                            path.moveTo(newPath);
                        } else {
                            if (moveIfExistMode != null) {
                                if (moveIfExistMode.equalsIgnoreCase(Constants.MOVE_IF_EXIST_KEEP)) {
                                    destination += "_" + UUID.randomUUID().toString();
                                    path.moveTo(fsManager.resolveFile(destination, opts));
                                } else if (moveIfExistMode.equalsIgnoreCase(Constants.MOVE_IF_EXIST_OVERWRITE)) {
                                    int fileDeleted = newPath.delete(Selectors.SELECT_SELF);
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(fileDeleted + " file successfully deleted in " + destination);
                                    }
                                    path.moveTo(newPath);
                                } else {
                                    throw new ClientConnectorException("The file at " + newPath.getURL().toString() +
                                            " already exists or it is a directory and the mode used for if file exist "
                                            + moveIfExistMode + " is not supported. ");
                                }
                            } else {
                                throw new ClientConnectorException("The file at " + newPath.getURL().toString() +
                                        " already exists or it is a directory");
                            }

                        }
                    } else {
                        throw new ClientConnectorException(
                                "Failed to move file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.READ:
                    if (path.exists()) {
                        //TODO: Do not assume 'path' always refers to a file
                        long fileContentLastModifiedTime;
                        do {
                            fileContentLastModifiedTime = path.getContent().getLastModifiedTime();
                            Thread.sleep(readWaitTimeout);
                        } while (fileContentLastModifiedTime < path.getContent().getLastModifiedTime());
                        String scheme = UriParser.extractScheme(fileURI);
                        String filePath;
                        String fileExtension;
                        if (scheme == null || scheme.equals(Constants.PROTOCOL_FILE)) {
                            filePath = path.getName().getPath();
                            fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);
                            bufferedReader = Files.newBufferedReader(Paths.get(filePath));
                        } else {
                            String fileName = path.getName().getBaseName();
                            filePath = path.getName().getPath();
                            fileExtension = fileName.substring(fileName.lastIndexOf(".") + 1);
                            bufferedReader = new BufferedReader(
                                    new InputStreamReader(path.getContent().getInputStream(), StandardCharsets.UTF_8));
                        }

                        String mode = map.get(Constants.MODE);
                        if (Constants.MODE_TYPE_LINE.equalsIgnoreCase(mode) &&
                                !fileExtension.equalsIgnoreCase(Constants.BINARY_FILE_EXTENSION)) {
                            boolean readOnlyHeader = Boolean.parseBoolean(map.get(Constants.READ_ONLY_HEADER));
                            boolean readOnlyTrailer = Boolean.parseBoolean(map.get(Constants.READ_ONLY_TRAILER));
                            boolean trailerSkipped = Boolean.parseBoolean(map.get(Constants.SKIP_TRAILER));
                            boolean headerSkipped = !Boolean.parseBoolean(map.get(Constants.HEADER_PRESENT));
                            String line;
                            BinaryCarbonMessage message;
                            line = bufferedReader.readLine();
                            if (readOnlyHeader && line != null) {
                                message = new BinaryCarbonMessage(ByteBuffer.
                                        wrap(line.getBytes(StandardCharsets.UTF_8)), true);
                                message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                        org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                message.setProperty(
                                        org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                        filePath);
                                message.setProperty(
                                        org.wso2.transport.file.connector.server.util.Constants.EOF,
                                        true);
                                carbonMessageProcessor.receive(message, carbonCallback);
                            } else if (readOnlyTrailer && line != null) {
                                String lastLine;
                                if (!Boolean.parseBoolean(map.get(Constants.HEADER_PRESENT))) {
                                    message = new BinaryCarbonMessage(ByteBuffer.
                                            wrap(line.getBytes(StandardCharsets.UTF_8)), true);
                                    message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                            org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                            filePath);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.EOF,
                                            true);
                                } else {
                                    message = null;
                                }
                                while ((line = bufferedReader.readLine()) != null) {
                                    lastLine = line;
                                    message = new BinaryCarbonMessage(ByteBuffer.
                                            wrap(lastLine.getBytes(StandardCharsets.UTF_8)), true);
                                    message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                            org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                            filePath);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.EOF,
                                            true);
                                }
                                if (null != message) {
                                    carbonMessageProcessor.receive(message, carbonCallback);
                                }
                            } else if (trailerSkipped && line != null) {
                                boolean skipSendingLine = Boolean.parseBoolean(map.get(Constants.HEADER_PRESENT));
                                while (line != null) {
                                    message = new BinaryCarbonMessage(ByteBuffer.
                                            wrap(line.getBytes(StandardCharsets.UTF_8)), true);
                                    message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                            org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                            filePath);
                                    message.setProperty(org.wso2.transport.file.connector.server.util.
                                            Constants.EOF, false);
                                    line = bufferedReader.readLine();
                                    while (line != null && line.isEmpty()) {
                                        line = bufferedReader.readLine();
                                    }

                                    if (!skipSendingLine) {
                                        if (line != null) {
                                            bufferedReader.mark(0);
                                            String nextLine = bufferedReader.readLine();
                                            boolean isEOFAfterNextLine = nextLine == null;
                                            if (isEOFAfterNextLine) {
                                                message.setProperty(org.wso2.transport.file.connector.server.util.
                                                                Constants.EOF, true);
                                            }
                                            bufferedReader.reset();
                                            carbonMessageProcessor.receive(message, carbonCallback);
                                            if (isEOFAfterNextLine) {
                                                break;
                                            }
                                        }
                                    } else {
                                        skipSendingLine = false;
                                    }
                                }
                            } else {
                                while (line != null) {
                                    message = new BinaryCarbonMessage(ByteBuffer.
                                            wrap(line.getBytes(StandardCharsets.UTF_8)), true);
                                    message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                            org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                    message.setProperty(
                                            org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                            filePath);
                                    line = bufferedReader.readLine();
                                    while (line != null && line.isEmpty()) {
                                        line = bufferedReader.readLine();
                                    }
                                    if (line == null) {
                                        message.setProperty(org.wso2.transport.file.connector.server.util.Constants.EOF,
                                                true);
                                    } else {
                                        message.setProperty(org.wso2.transport.file.connector.server.util.Constants.EOF,
                                                false);
                                    }
                                    if (headerSkipped) {
                                        carbonMessageProcessor.receive(message, carbonCallback);
                                    } else {
                                        headerSkipped = true;
                                    }
                                }
                            }
                        } else if (Constants.MODE_TYPE_BINARY_CHUNKED.equalsIgnoreCase(mode)) {
                            inputStream = path.getContent().getInputStream();
                            // Read a file in BINARY_CHUNKED mode
                            long size = path.getContent().getSize();
                            int bufferSize = Integer.parseInt(map.get(Constants.BUFFER_SIZE_IN_BINARY_CHUNKED));
                            logger.debug("Reading " + path.getName() + " on " + mode + ". Chunk size: " + bufferSize +
                                    " bytes, File size: " + size + " bytes.");
                            byte[] buffer = new byte[bufferSize];
                            int sequenceNumber = 1;
                            int readLength;
                            BinaryCarbonMessage message;
                            bufferedReader = Files.newBufferedReader(Paths.get(filePath));
                            while ((readLength = inputStream.read(buffer)) != -1) {
                                // Adjust the buffer size if the read bytes are less than the buffer size.
                                if (readLength != buffer.length) {
                                    buffer = Arrays.copyOfRange(buffer, 0, readLength);
                                }
                                message = new BinaryCarbonMessage(ByteBuffer.
                                        wrap(buffer), true);
                                message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                        org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                                message.setProperty(org.wso2.transport.file.connector.server.util.Constants.FILE_NAME,
                                        path.getName().getBaseName());
                                message.setProperty(
                                        org.wso2.transport.file.connector.server.util.Constants.CONTENT_LENGTH, size);
                                message.setProperty(
                                        org.wso2.transport.file.connector.server.util.Constants.SEQUENCE_NUMBER,
                                        sequenceNumber++);
                                message.setProperty(org.wso2.transport.file.connector.server.util.Constants.EOF,
                                        !(inputStream.available() > 0));
                                carbonMessageProcessor.receive(message, carbonCallback);
                                buffer = new byte[bufferSize];
                            }
                            logger.debug("Reading " + path.getPublicURIString() + " is completed.");
                        } else {
                            inputStream = path.getContent().getInputStream();
                            BinaryCarbonMessage message = new BinaryCarbonMessage(ByteBuffer.
                                    wrap(toByteArray(inputStream)), true);
                            message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                                    org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                            message.setProperty(org.wso2.transport.file.connector.server.util.Constants.FILE_PATH,
                                    filePath);
                            carbonMessageProcessor.receive(message, carbonCallback);
                        }
                    } else {
                        throw new ClientConnectorException(
                                "Failed to read file: " + path.getName().getURI() + " not found");
                    }
                    break;
                case Constants.EXISTS:
                    TextCarbonMessage message = new TextCarbonMessage(Boolean.toString(path.exists()));
                    message.setProperty(org.wso2.carbon.messaging.Constants.DIRECTION,
                            org.wso2.carbon.messaging.Constants.DIRECTION_RESPONSE);
                    carbonMessageProcessor.receive(message, carbonCallback);
                    break;
                default:
                    return false;
            }
        } catch (RuntimeException e) {
            throw new ClientConnectorException("Runtime Exception occurred : " + e.getMessage(), e);
        } catch (Exception e) {
            throw new ClientConnectorException("Exception occurred while processing file: " + e.getMessage(), e);
        } finally {
            closeQuietly(bufferedReader);
            closeQuietly(inputStream);
            closeQuietly(outputStream);
        }
        return true;
    }

    @Override
    public String getProtocol() {
        // TODO: Revisit this
        return Constants.PROTOCOL_FILE;
    }

    /**
     * Obtain a byte[] from an input stream
     *
     * @param input The input stream that the data should be obtained from
     * @return byte[] The byte array of data obtained from the input stream
     * @throws IOException
     */
    private static byte[] toByteArray(InputStream input) throws IOException {
        long count = 0L;
        byte[] buffer = new byte[4096];
        int n1;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (; -1 != (n1 = input.read(buffer)); count += (long) n1) {
            output.write(buffer, 0, n1);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(count + " bytes read");
        }
        byte[] bytes = output.toByteArray();
        closeQuietly(output);
        return bytes;
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
            logger.error("Error occurred when closing stream", e);
        }
    }

    @Override
    public void setMessageProcessor(CarbonMessageProcessor carbonMessageProcessor) {
        this.carbonMessageProcessor = carbonMessageProcessor;
    }
}
