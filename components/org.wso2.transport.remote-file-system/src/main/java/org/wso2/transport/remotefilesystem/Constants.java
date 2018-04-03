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

package org.wso2.transport.remotefilesystem;

/**
 * This class contains the constants related to File transport.
 */
public final class Constants {

    public static final String TRANSPORT_FILE_URI = "dirURI";
    public static final String FILE_SORT_PARAM = "fileSortAttribute";
    public static final String FILE_SORT_VALUE_NAME = "name";
    public static final String FILE_SORT_VALUE_SIZE = "size";
    public static final String FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP = "lastModifiedTimestamp";
    public static final String FILE_SORT_ORDER = "fileSortAscending";
    public static final String FILE_NAME_PATTERN = "fileNamePattern";
    public static final String ACTION_AFTER_PROCESS = "actionAfterProcess";
    public static final String ACTION_AFTER_FAILURE = "actionAfterFailure";
    public static final String MOVE_TIMESTAMP_FORMAT = "moveTimestampFormat";
    public static final String MOVE_AFTER_PROCESS = "moveAfterProcess";
    public static final String MOVE_AFTER_FAILURE = "moveAfterFailure";
    public static final String FORCE_CREATE_FOLDER = "createMoveDir";
    public static final String PARALLEL = "parallel";
    public static final String THREAD_POOL_SIZE = "threadPoolSize";
    public static final String FILE_PROCESS_COUNT = "perPollFileCount";
    public static final String CREATE_FOLDER = "create-folder";

    public static final String ACTION_MOVE = "MOVE";
    public static final String ACTION_DELETE = "DELETE";
    public static final String ACTION_NONE = "NONE";

    public static final String SCHEME = "VFS_SCHEME";
    public static final String SFTP_PREFIX = "sftp";
    public static final String SCHEME_SFTP = "sftp";
    public static final String SCHEME_FTP = "ftp";

    public static final String FILE_TYPE = "filetype";
    public static final String BINARY_TYPE = "BINARY";
    public static final String LOCAL_TYPE = "LOCAL";
    public static final String ASCII_TYPE = "ASCII";
    public static final String EBCDIC_TYPE = "EBCDIC";

    public static final String PROTOCOL_FTP = "ftp";

    public static final String URI = "uri";
    public static final String ACTION = "action";

    public static final String APPEND = "append";
    public static final String MKDIR = "mkdir";
    public static final String RMDIR = "rmdir";
    public static final String PUT = "put";
    public static final String GET = "get";
    public static final String DELETE = "delete";
    public static final String RENAME = "rename";
    public static final String SIZE = "size";
    public static final String LIST = "list";


    public static final String FTP_PASSIVE_MODE = "FTP_PASSIVE_MODE";
    public static final String PROTOCOL = "PROTOCOL";
    public static final String DESTINATION = "destination";


    private Constants() {
    }

    /**
     * Enum for SFTP file options.
     */
    public enum SftpFileOption {
        Identities,
        UserDirIsRoot,
        IdentityPassPhrase,
        AvoidPermissionCheck;

        SftpFileOption() {
        }
    }
}
