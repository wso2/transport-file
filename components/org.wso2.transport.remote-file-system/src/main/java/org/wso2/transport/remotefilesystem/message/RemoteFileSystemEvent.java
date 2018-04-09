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

/**
 * This class represent the events that happen in remote file system.
 */
public class RemoteFileSystemEvent extends RemoteFileSystemBaseMessage {

    private final String uri;
    private final String baseName;
    private final String path;
    private long fileSize;
    private long lastModifiedTime;

    public RemoteFileSystemEvent(String uri, String baseName, String path) {
        this.uri = uri;
        this.baseName = baseName;
        this.path = path;
    }

    /**
     * This will return absolute path including the schema, username and masked password.
     *
     * @return The absolute path.
     */
    public String getUri() {
        return this.uri;
    }

    /**
     * This will return file or folder name.
     *
     * @return File or folder name.
     */
    public String getBaseName() {
        return baseName;
    }

    /**
     * This will return relative path from the root folder.
     *
     * @return The relative path from root folder.
     */
    public String getPath() {
        return path;
    }

    /**
     * Determines the size of the file, in bytes.
     *
     * @return File size in bytes.
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * Determines the last-modified timestamp of the file.
     *
     * @return Last-modified timestamp.
     */
    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }
}
