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

package org.wso2.carbon.transport.file.test;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.transport.file.connector.client.sender.FileClientConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Test case that tests the file server connector functionality.
 */
public class FileClientConnectorTestCase {

    private final byte[] bytes = "This is a Sample Text".getBytes();

    @BeforeMethod
    public void createTempDir() {
        File temp = new File("temp");
        if (temp.exists()) {
            deleteDir(temp);
        } else {
            temp.mkdir();
        }
    }

    @AfterMethod
    public void deleteTempDir() {
        File temp = new File("temp");
        if (temp.exists()) {
            deleteDir(temp);
        }

        File tempDir = new File("tempDir");
        if (tempDir.exists()) {
            deleteDir(tempDir);
        }
    }

    @Test
    public void fileCreateTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "create");
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertTrue(testFile.exists(), "file not created");
        Assert.assertTrue(testFile.isFile(), "created directory instead of file");
    }

    @Test
    public void dirCreateTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/dir1/dir2/dir3");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "create");
        propertyMap.put("create-folder", "true");
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertTrue(testFile.exists(), "file not created");
        Assert.assertTrue(testFile.isDirectory(), "created file instead of directory");
    }

    @Test
    public void fileWriteTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "write");
        BinaryCarbonMessage binaryCarbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(bytes), true);
        fileClientConnector.send(binaryCarbonMessage, null, propertyMap);
        Assert.assertEquals(bytes, IOUtils.toByteArray(new FileInputStream(testFile)), "Wrong Content written to File");
    }

    @Test
    public void fileCopyTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        File destFile = new File("temp/copy/copied.txt");
        OutputStream outputStream = new FileOutputStream(testFile);
        IOUtils.write(bytes, outputStream);
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "copy");
        propertyMap.put("destination", destFile.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertTrue(testFile.exists(), "Original file not found");
        Assert.assertTrue(destFile.exists(), "File Not Copied to new location");
        Assert.assertEquals(bytes, IOUtils.toByteArray(new FileInputStream(destFile)), "Wrong Content copied to File");
    }

    @Test(expectedExceptions = ClientConnectorException.class)
    public void copyNonExistentFile()
            throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        File destFile = new File("temp/copy/copied.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "copy");
        propertyMap.put("destination", destFile.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
    }

    @Test
    public void dirCopyTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testDir = new File("temp/dir");
        testDir.mkdir();
        File testFile = new File("temp/dir/test.txt");
        File destDir = new File("temp/copy/dir");
        File destFile = new File("temp/copy/dir/test.txt");
        OutputStream outputStream = new FileOutputStream(testFile);
        IOUtils.write(bytes, outputStream);
        String fileURI = testDir.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "copy");
        propertyMap.put("destination", destDir.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertTrue(testFile.exists(), "Original file not found");
        Assert.assertTrue(destFile.exists(), "File Not Copied to new location");
        Assert.assertEquals(bytes, IOUtils.toByteArray(new FileInputStream(testFile)), "Wrong Content copied to File");
    }

    @Test
    public void fileMoveTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        File destFile = new File("temp/move/moved.txt");
        OutputStream outputStream = new FileOutputStream(testFile);
        IOUtils.write(bytes, outputStream);
        String fileURI = testFile.getAbsolutePath();
        new File("temp/move").mkdir();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "move");
        propertyMap.put("destination", destFile.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertFalse(testFile.exists(), "Original file not Deleted when moving");
        Assert.assertTrue(destFile.exists(), "File Not Moved to new location");
        Assert.assertEquals(bytes, IOUtils.toByteArray(new FileInputStream(destFile)),
                            "Wrong Content in moved to File");
    }

    @Test(expectedExceptions = ClientConnectorException.class)
    public void moveNonExistentFile()
            throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        File destFile = new File("temp/move/moved.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "move");
        propertyMap.put("destination", destFile.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
    }

    @Test
    public void dirMoveTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testDir = new File("temp/dir");
        testDir.mkdir();
        File testFile = new File("temp/dir/test.txt");
        File destDir = new File("temp/move/dir");
        File destFile = new File("temp/move/dir/test.txt");
        OutputStream outputStream = new FileOutputStream(testFile);
        IOUtils.write(bytes, outputStream);
        String fileURI = testDir.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "move");
        propertyMap.put("destination", destDir.getAbsolutePath());
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertFalse(testFile.exists(), "Original file not deleted");
        Assert.assertTrue(destFile.exists(), "File Not Moved to new location");
        Assert.assertEquals(bytes, IOUtils.toByteArray(new FileInputStream(destFile)), "Wrong Content moved to File");
    }

    @Test
    public void fileDeleteTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        String fileURI = testFile.getAbsolutePath();
        testFile.createNewFile();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "delete");
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertFalse(testFile.exists(), "File not Deleted");
    }

    @Test(expectedExceptions = ClientConnectorException.class)
    public void deleteNonExistentFile()
            throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "delete");
        fileClientConnector.send(null, null, propertyMap);
    }

    @Test
    public void dirDeleteTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testDir = new File("temp/dir");
        testDir.mkdir();
        new File("temp/dir/test.txt").createNewFile();
        String fileURI = testDir.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "delete");
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertFalse(testDir.exists(), "Folder not Deleted");
    }

    @Test
    public void fileReadTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        ClassLoader classLoader = getClass().getClassLoader();
        File testFile = new File(classLoader.getResource("test.txt").getFile());
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "read");
        FileMessageProcessor messageProcessor = new FileMessageProcessor();
        fileClientConnector.setMessageProcessor(messageProcessor);
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertEquals(IOUtils.toByteArray(new FileInputStream(testFile)),
                            messageProcessor.getBinaryCarbonMessage().readBytes().array(),
                            "Wrong Content written to File");
    }

    @Test(expectedExceptions = ClientConnectorException.class)
    public void readNonExistentFile()
            throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        File testFile = new File("temp/test.txt");
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "read");
        fileClientConnector.send(null, null, propertyMap);
    }

    @Test
    public void fileExistTestCase() throws ClientConnectorException, IOException {
        FileClientConnector fileClientConnector = new FileClientConnector();
        Map<String, String> propertyMap = new HashMap<>();
        ClassLoader classLoader = getClass().getClassLoader();
        File testFile = new File(classLoader.getResource("test.txt").getFile());
        String fileURI = testFile.getAbsolutePath();
        propertyMap.put("uri", fileURI);
        propertyMap.put("action", "exists");
        FileMessageProcessor messageProcessor = new FileMessageProcessor();
        fileClientConnector.setMessageProcessor(messageProcessor);
        fileClientConnector.send(null, null, propertyMap);
        Assert.assertTrue(messageProcessor.getTextCarbonMessage().getText().equalsIgnoreCase("true"),
                          "returns false when the file does exist");
    }

    private void deleteDir(File dir) {
        String[] entries = dir.list();
        if (entries != null && entries.length != 0) {
            for (String s : entries) {
                File currentFile = new File(dir.getPath(), s);
                if (currentFile.isDirectory()) {
                    deleteDir(currentFile);
                }
                currentFile.delete();
            }
        }
        dir.delete();
    }

}
