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

package org.wso2.carbon.transport.localfilesystem.server.connector.util;

/**
 * This class contains the constants related to File transport.
 */
public final class Constants {

    public static final String PROTOCOL_FILE_SYSTEM = "fs";
    public static final String PROTOCOL_FILE = "file";

    //file connector parameters
    public static final String TRANSPORT_FILE_FILE_URI = "dirURI";
    public static final String FILE_TRANSPORT_PROPERTY_SERVICE_NAME = "TRANSPORT_FILE_SERVICE_NAME";
    public static final String DIRECTORY_WATCH_EVENTS = "events";
    public static final String DIRECTORY_WATCH_RECURSIVE = "recursive";
}
