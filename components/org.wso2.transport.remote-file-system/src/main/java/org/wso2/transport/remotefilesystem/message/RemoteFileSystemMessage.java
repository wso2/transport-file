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

package org.wso2.transport.remotefilesystem.message;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This represent the message that hold payload and other attributes.
 */
public class RemoteFileSystemMessage extends RemoteFileSystemBaseMessage {

    private ByteBuffer bytes;
    private InputStream inputStream;
    private String text;
    private long size;
    private String[] childNames;
    private boolean directory;

    public RemoteFileSystemMessage(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public RemoteFileSystemMessage(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public RemoteFileSystemMessage(String text) {
        this.text = text;
    }

    public RemoteFileSystemMessage(long size) {
        this.size = size;
    }

    public RemoteFileSystemMessage(final String[] childNames) {
        this.childNames = childNames.clone();
    }

    public RemoteFileSystemMessage(boolean isDirectory) {
        this.directory = isDirectory;
    }

    public ByteBuffer getBytes() {
        return bytes;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public String getText() {
        return text;
    }

    public long getSize() {
        return size;
    }

    public String[] getChildNames() {
        return childNames.clone();
    }

    public boolean isDirectory() {
        return directory;
    }
}
