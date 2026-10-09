/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.transport.file.test;

import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the VFS client connector against an FTP server whose directories are created after the connector's
 * first use, which is what the file source does when it moves processed files.
 */
public class VFSClientConnectorFtpTestCase {

    private static final String USER = "wso2";
    private static final String PASSWORD = "wso2123";

    private FakeFtpServer ftpServer;
    private FileSystem fileSystem;
    private int serverPort;

    @BeforeClass
    public void startServer() {
        ftpServer = new FakeFtpServer();
        ftpServer.setServerControlPort(0);
        ftpServer.addUserAccount(new UserAccount(USER, PASSWORD, "/"));
        fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/"));
        ftpServer.setFileSystem(fileSystem);
        ftpServer.start();
        serverPort = ftpServer.getServerControlPort();
    }

    @AfterClass
    public void stopServer() {
        ftpServer.stop();
    }

    @Test
    public void moveIntoMissingFolderOfDirectoryCreatedAfterFirstUse() throws ClientConnectorException {
        for (String run : new String[]{"runA", "runB", "runC", "runD"}) {
            fileSystem.add(new DirectoryEntry("/" + run));
            fileSystem.add(new DirectoryEntry("/" + run + "/in"));
            fileSystem.add(new FileEntry("/" + run + "/in/sales.csv", "a,b"));

            String base = "ftp://" + USER + ":" + PASSWORD + "@localhost:" + serverPort + "/" + run;
            Map<String, Object> properties = new HashMap<>();
            properties.put("uri", base + "/in/sales.csv");
            VFSClientConnector connector = new VFSClientConnector();
            connector.init(null, null, properties);

            Map<String, String> options = new HashMap<>();
            options.put("uri", base + "/in/sales.csv");
            options.put("action", "move");
            options.put("destination", base + "/processed/sales.csv");
            connector.send(null, null, options);

            Assert.assertTrue(fileSystem.exists("/" + run + "/processed/sales.csv"), run + ": file not moved");
            Assert.assertFalse(fileSystem.exists("/" + run + "/in/sales.csv"), run + ": source not removed");
        }
    }
}
