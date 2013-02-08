/*
 * =============================================================================
 * 
 *   This software is part of the DenodoConnect component collection.
 *   
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 * =============================================================================
 */
package com.denodo.connect.hdfs.wrapper.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsSimpleFileTest {

    public static void main(String[] args) {
        String host = "192.168.73.132";
        int port = 8020;

        // Establishing configuration
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);
        String inputFilePath = "/wordcount/output";
        String columnDelimiter = "\t";
        // File to read
        Path inputPath = new Path(inputFilePath);
        FileSystem fileSystem;
        FSDataInputStream dataInputStream = null;
        List result = new ArrayList();
        try {
            fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(inputPath)) {
                FileStatus[] fss = fileSystem.listStatus(inputPath);
                for (FileStatus status : fss) {
                    Path path = status.getPath();
                    if (!status.isDir()) {
                        dataInputStream = fileSystem.open(path);
                        String line = "";
                        String[] row = new String[4];
                        while ((line = dataInputStream.readLine()) != null) {
                            String[] lineArray = line.split(columnDelimiter);
                            // columnDelimiter matches the key/value delimiter
                            if (lineArray.length == 2) {
                                row[0] = lineArray[0];
                                row[1] = lineArray[1];
                                row[2] = inputFilePath;
                                row[3] = columnDelimiter;
                                System.out.println(row[0] + " - " + row[1] + " - " + row[2] + " - " + row[3]);
                                result.add(row);
                            } else {
                                System.out.println("Column delimiter matches the key/value delimiter");
                                break;
                            }
                        }
                    }
                }
                if (dataInputStream != null)
                    dataInputStream.close();
                // try {
                // Delete path recursively after reading
                // fileSystem.delete(inputPath, true);
                // System.out.println("Deleted path " + inputFilePath);
                // } catch (IOException e) {
                // e.printStackTrace();
                // } finally {
                if (fileSystem != null)
                    fileSystem.close();
                // }
            } else {
                System.out.println("Path not found " + inputFilePath);
                if (fileSystem != null)
                    fileSystem.close();
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
