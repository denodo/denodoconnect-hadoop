/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2013, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Test class to create a sample delimited text file
 *
 */
public class DelimitedTextFileWriteDemo {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://" + "192.168.25.128:8020/user/sandbox/text";
        Configuration conf = new Configuration();

        // create am HDFS file system
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        // create an output stream to write to a new file in hdfs
        Path outputPath = new Path(uri);
        OutputStream outputStream = fs.create(outputPath);

        // send content to file via compressed output stream using .write
        // methods
        outputStream.write(new String("1\tOne, two, buckle my shoe\n").getBytes());
        outputStream.write(new String("2\tThree, four, shut the door\n").getBytes());
        outputStream.write(new String("3\tFive, six, pick up sticks\n").getBytes());
        outputStream.write(new String("4\tSeven, eight, lay them straight\n").getBytes());
        outputStream.write(new String("5\tNine, ten, a big fat hen\n").getBytes());

        // close out stream
        IOUtils.closeStream(outputStream);
    }
}