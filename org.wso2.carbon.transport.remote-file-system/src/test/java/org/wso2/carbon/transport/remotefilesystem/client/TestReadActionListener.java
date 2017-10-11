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

package org.wso2.carbon.transport.remotefilesystem.client;

import org.wso2.carbon.transport.remotefilesystem.listener.RemoteFileSystemListener;
import org.wso2.carbon.transport.remotefilesystem.message.RemoteFileSystemBaseMessage;
import org.wso2.carbon.transport.remotefilesystem.message.RemoteFileSystemMessage;

import java.util.concurrent.CountDownLatch;

/**
 * Test {@link RemoteFileSystemListener} implementation for testing purpose.
 */
public class TestReadActionListener implements RemoteFileSystemListener {

    private CountDownLatch latch;
    private String content;
    private Throwable throwable;

    TestReadActionListener(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onMessage(RemoteFileSystemBaseMessage remoteFileSystemBaseMessage) {
        RemoteFileSystemMessage message = (RemoteFileSystemMessage) remoteFileSystemBaseMessage;
        content = new String(message.getBytes().array());
        latch.countDown();
    }

    @Override
    public void onError(Throwable throwable) {
        this.throwable = throwable;
        latch.countDown();
    }

    @Override
    public void done() {
        latch.countDown();
    }

    String getContent() {
        return content;
    }

    Throwable getThrowable() {
        return throwable;
    }
}
