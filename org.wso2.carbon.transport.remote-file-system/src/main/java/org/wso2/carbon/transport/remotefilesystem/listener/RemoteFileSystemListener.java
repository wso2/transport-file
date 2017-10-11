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

package org.wso2.carbon.transport.remotefilesystem.listener;

import org.wso2.carbon.transport.remotefilesystem.message.RemoteFileSystemBaseMessage;

/**
 * Allows to get notifications of connectors.
 */
public interface RemoteFileSystemListener {

    /**
     * Transport will trigger this method when for each file system notification.
     *
     * @param remoteFileSystemMessage contains the msg data.
     */
    void onMessage(RemoteFileSystemBaseMessage remoteFileSystemMessage);

    /**
     * Notify error event triggered by transport to the listener.
     *
     * @param throwable contains the error details of the event.
     */
    void onError(Throwable throwable);

    /**
     * Notify to the caller once the underlying task is successfully finished. Error situation need to handle through
     * {@link #onError(Throwable)} method.
     */
    void done();
}
