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

package org.wso2.carbon.transport.file.connector.server.util;

/**
 * This class contains the constants related to File transport.
 */
public final class Constants {

    public static final String ANNOTATION_PROTOCOL = "protocol";
    //todo: this should be a common annotation for all server connectors.
    public static final String PROTOCOL_NAME = "file";
    public static final String AFTER_ACTION_MOVE = "MOVE";
    public static final String FILE_TRANSPORT_PROPERTY_SERVICE_NAME = "transport.file.ServiceName";

    /**
     * Following constants are coming from earlier VFS Transport.
     */
    public static final String TRANSPORT_FILE_ACTION_AFTER_PROCESS = "actionAfterProcess";
    public static final String TRANSPORT_FILE_ACTION_AFTER_FAILURE = "actionAfterFailure";
    public static final String TRANSPORT_FILE_MOVE_AFTER_PROCESS = "moveAfterProcess";
    public static final String TRANSPORT_FILE_MOVE_AFTER_FAILURE = "moveAfterFailure";
    public static final String TRANSPORT_FILE_FILE_URI = "fileURI";
    public static final String TRANSPORT_FILE_FILE_NAME_PATTERN = "fileNamePattern";
    public static final String TRANSPORT_FILE_LOCKING = "locking";
    public static final String TRANSPORT_FILE_LOCKING_DISABLED = "disable";
    public static final String TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT = "moveTimestampFormat";
    public static final String MAX_RETRY_COUNT = "maxRetryCount";
    public static final String FORCE_CREATE_FOLDER = "createFolder";
    public static final String RECONNECT_TIMEOUT = "reconnectTimeout";
    public static final String SUBFOLDER_TIMESTAMP = "subFolderTimestampFormat";
    public static final String FILE_SORT_PARAM = "fileSortAttribute";
    public static final String FILE_SORT_VALUE_NAME = "Name";
    public static final String FILE_SORT_VALUE_SIZE = "Size";
    public static final String FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP = "Lastmodifiedtimestamp";
    public static final String FILE_SORT_ORDER = "fileSortAsscending";
    public static final String TRANSPORT_FILE_INTERVAL = "fileProcessInterval";
    public static final String TRANSPORT_FILE_COUNT = "fileProcessCount";
    public static final String TRANSPORT_AUTO_LOCK_RELEASE = "autoLockRelease";
    public static final String TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL =
            "autoLockReleaseInterval";
    public static final String TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE =
            "lockReleaseSameNode";
    public static final String FILE_PATH = "FILE_PATH";
    public static final String FILE_URI = "FILE_URI";
    public static final String FILE_NAME = "FILE_NAME";
    public static final String FILE_LENGTH = "FILE_LENGTH";
    public static final String LAST_MODIFIED = "LAST_MODIFIED";
    public static final String SCHEME = "VFS_SCHEME";
    public static final String SFTP_PREFIX = "sftp";
    public static final String SCHEME_SFTP = "sftp";
    public static final String FILE_TYPE = "filetype";
    public static final String BINARY_TYPE = "BINARY";
    public static final String LOCAL_TYPE = "LOCAL";
    public static final String ASCII_TYPE = "ASCII";
    public static final String EBCDIC_TYPE = "EBCDIC";

    public Constants() {
    }

    /**
     * Enum for SFTP file options.
     */
    public enum SftpFileOption {
        Identities,
        UserDirIsRoot,
        IdentityPassPhrase;

        private SftpFileOption() {
        }
    }
}
