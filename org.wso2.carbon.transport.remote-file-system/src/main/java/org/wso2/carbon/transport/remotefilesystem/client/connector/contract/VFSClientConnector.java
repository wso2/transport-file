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

package org.wso2.carbon.transport.remotefilesystem.client.connector.contract;

import org.wso2.carbon.transport.remotefilesystem.message.RemoteFileSystemMessage;

/**
 * A Client Connector for remote file systems using the Apache VFS library.
 */
public interface VFSClientConnector {

    /**
     * Send {@link RemoteFileSystemMessage} to target file system using VFS.
     *
     * @param message {@link RemoteFileSystemMessage} which contains relevant information which need to send to target
     *                file system.
     * @return {@link VFSClientConnectorFuture} instance which contains response from the target file system.
     */
    VFSClientConnectorFuture send(RemoteFileSystemMessage message);
}
