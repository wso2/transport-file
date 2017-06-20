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

package org.wso2.carbon.transport.file.connector.sender;

/**
 * This class contains the constants related to File transport.
 */
public final class Constants {

    public static final String PROTOCOL_NAME = "file";

    public static final String APPEND = "append";
    public static final String FILE_URI = "uri";
    public static final String ACTION = "action";
    public static final String CREATE = "create";
    public static final String WRITE = "write";
    public static final String DELETE = "delete";
    public static final String COPY = "copy";
    public static final String MOVE = "move";
    public static final String READ = "read";
    public static final String EXISTS = "exists";

    private Constants() {
    }
}
