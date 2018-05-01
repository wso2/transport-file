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

package org.wso2.transport.remotefilesystem.server;

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
import org.wso2.transport.remotefilesystem.Constants;
import org.wso2.transport.remotefilesystem.RemoteFileSystemConnectorFactory;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.impl.RemoteFileSystemConnectorFactoryImpl;
import org.wso2.transport.remotefilesystem.message.FileInfo;
import org.wso2.transport.remotefilesystem.server.connector.contract.RemoteFileSystemServerConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test case that tests the file server connector functionality.
 */
public class RemoteFileSystemServerConnectorTestCase {

    private FakeFtpServer ftpServer;
    private FileSystem fileSystem;
    private int serverPort;
    private static String username = "wso2";
    private static String password = "wso2123";
    private static String rootFolder = "/home/wso2";
    private static String file1 = "/home/wso2/file1.txt";
    private static String file2 = "/home/wso2/exe/run.exe";

    @BeforeClass
    public void init() {
        ftpServer = new FakeFtpServer();
        ftpServer.setServerControlPort(0);
        ftpServer.addUserAccount(new UserAccount(username, password, rootFolder));

        fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry(rootFolder));
        fileSystem.add(new FileEntry(file1, "some content 1234567890"));
        fileSystem.add(new FileEntry(file2, "Test Value"));
        ftpServer.setFileSystem(fileSystem);
        ftpServer.start();
        serverPort = ftpServer.getServerControlPort();
    }

    @Test(description = "Testing whether correctly getting the file path.")
    public void retrieveFileListTestCase() throws InterruptedException, RemoteFileSystemConnectorException {
        int expectedEventCount = 3;
        String newFile = "/home/wso2/file2.txt";
        List<String> fileNames = new ArrayList<>();
        fileNames.add(file1);
        fileNames.add(file2);
        fileNames.add(newFile);
        Map<String, String> parameters = getPropertyMap();
        CountDownLatch latch = new CountDownLatch(1);
        RemoteFileSystemConnectorFactory connectorFactory = new RemoteFileSystemConnectorFactoryImpl();
        TestServerRemoteFileSystemListener fileSystemListener = new TestServerRemoteFileSystemListener(latch,
                expectedEventCount);
        RemoteFileSystemServerConnector testConnector = connectorFactory
                .createServerConnector("TestService", parameters, fileSystemListener);
        testConnector.poll();
        fileSystem.add(new FileEntry(newFile));
        int i = 0;
        while (!fileSystem.exists(newFile)) {
            Thread.sleep(1000);
            if (i++ > 10) {
                break;
            }
        }
        testConnector.poll();
        List<FileInfo> fileList = fileSystemListener.getFileInfos();
        i = 0;
        while (fileList.size() < 3) {
            testConnector.poll();
            Thread.sleep(1000);
            if (i++ > 10) {
                break;
            }
        }
        latch.await(1, TimeUnit.MINUTES);
        if (fileList.size() == 0) {
            Assert.fail("File event didn't triggered.");
        }
        Assert.assertEquals(fileList.size(), expectedEventCount, "Generated events count mismatch with the expected.");

        for (FileInfo info : fileList) {
            Assert.assertTrue(fileNames.contains(info.getPath()));
        }
        fileSystem.delete("/home/wso2/file2.txt");
    }

    @Test(expectedExceptions = RemoteFileSystemConnectorException.class,
          expectedExceptionsMessageRegExp = "Failed to initialize File server connector for Service: TestService")
    public void invalidRootFolderTestCase() throws InterruptedException, RemoteFileSystemConnectorException {
        int expectedEventCount = 1;
        Map<String, String> parameters = new HashMap<>();
        parameters.put(Constants.URI,
                "ftp://" + username + ":" + password + "@localhost:" + serverPort + "/home/wso2/file1.txt");

        CountDownLatch latch = new CountDownLatch(1);
        RemoteFileSystemConnectorFactory connectorFactory = new RemoteFileSystemConnectorFactoryImpl();
        TestServerRemoteFileSystemListener fileSystemListener = new TestServerRemoteFileSystemListener(latch,
                expectedEventCount);
        try {
            connectorFactory.createServerConnector("TestService", parameters, fileSystemListener);
        } catch (RemoteFileSystemConnectorException e) {
            Assert.assertEquals(e.getCause().getMessage(),
                    "[TestService] File system server connector is used to listen to a folder. "
                            + "But the given path does not refer to a folder.");
            throw e;
        }
        latch.await(1, TimeUnit.MINUTES);
        Assert.assertNotNull(fileSystemListener.getThrowable(), "Expected exception didn't throw.");
    }

    @AfterClass
    public void cleanup() {
        if (ftpServer != null && ftpServer.isStarted()) {
            ftpServer.stop();
        }
    }

    private Map<String, String> getPropertyMap() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(Constants.URI, buildConnectionURL());
        parameters.put(Constants.USER_DIR_IS_ROOT, "false");
        parameters.put(Constants.PASSIVE_MODE, "true");
        parameters.put(Constants.AVOID_PERMISSION_CHECK, "true");
        return parameters;
    }

    private String buildConnectionURL() {
        return "ftp://" + username + ":" + password + "@localhost:" + serverPort + rootFolder;
    }
}
