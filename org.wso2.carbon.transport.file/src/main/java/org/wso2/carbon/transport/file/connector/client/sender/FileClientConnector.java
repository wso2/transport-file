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

package org.wso2.carbon.transport.file.connector.client.sender;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.StreamingCarbonMessage;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * FileClientConnector
 */
public class FileClientConnector implements ClientConnector {
    private static final Logger logger = LoggerFactory.getLogger(FileClientConnector.class);
    private String fileURI;
    private FileSystemManager fsManager;
    private FileSystemOptions opts = new FileSystemOptions();
    private CarbonMessageProcessor carbonMessageProcessor;

    @Override public boolean send(CarbonMessage carbonMessage, CarbonCallback carbonCallback)
            throws ClientConnectorException {
        return false;
    }

    @Override public boolean send(CarbonMessage carbonMessage, CarbonCallback carbonCallback, Map<String, String> map)
            throws ClientConnectorException {
        fileURI = map.get("uri");
        String action = map.get("action");
        FileType fileType;
        InputStream is = null;
        OutputStream os = null;
        try {
            fsManager = VFS.getManager();
            FileObject path = fsManager.resolveFile(fileURI, opts);
            fileType = path.getType();
            switch (action) {

                case "write":
                    if (!path.exists()) {
                        path.createFile();
                        path.refresh();
                        fileType = path.getType();
                    }
                    if (fileType == FileType.FILE) {
                        is = carbonMessage.getInputStream();
                        os = path.getContent().getOutputStream(true);
                        long bytesCopied = IOUtils.copy(is, os);
                        os.flush();

                        String bytes = Long.toString(bytesCopied);
                        logger.info(bytes + " bytes appended");
                    }
                    break;
                case "delete":
                    if (path.exists()) {
                        boolean isDeleted = path.delete();
                        if (isDeleted) {
                            logger.debug("File Successfully Deleted");
                        }
                    }
                    break;
                case "copy":
                    if (path.exists()) {
                        String destination = map.get("destination");
                        FileObject dest = fsManager.resolveFile(destination, opts);
                        dest.copyFrom(path, Selectors.SELECT_ALL);
                    }
                    break;
                case "move":
                    if (path.exists()) {
                        is = path.getContent().getInputStream();
                        String destination = map.get("destination");
                        FileObject newPath = fsManager.resolveFile(destination, opts);
                        if (!newPath.exists()) {
                            path.moveTo(newPath);
                        }
                    }
                    break;
                case "read":
                    if (path.exists()) {
                        is = path.getContent().getInputStream();
                        StreamingCarbonMessage message = new StreamingCarbonMessage(is);
                        carbonMessageProcessor.receive(message, carbonCallback);
                    }
                    break;
                case "isExist":
                    TextCarbonMessage message = new TextCarbonMessage(Boolean.toString(path.exists()));
                    carbonMessageProcessor.receive(message, carbonCallback);
                    break;
                default: return false;
            }
        } catch (IOException e) {
            throw new ClientConnectorException("Exception occurred while sending the message", e);
        } catch (Exception e) {
            throw new ClientConnectorException("Exception occurred while sending the callback", e);
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
        return true;
    }

    @Override public String getProtocol() {
        return "file";
    }

    @Override public void setMessageProcessor(CarbonMessageProcessor carbonMessageProcessor) {
        this.carbonMessageProcessor = carbonMessageProcessor;
    }
}
