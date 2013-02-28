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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.denodo.connect.hadoop.hdfs.wrapper.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.wrapper.reader.HDFSSequenceFileReader;

/**
 * Class to test the delimited SequenceFile reader
 *
 */
public class HdfsSequecenFileTest {

    public static void main(String[] args) {
        String hostIp = "192.168.25.128";
        String hostPort = "8020";
        String inputFilePath = "/user/sandbox/sequence.seq";
        Path path = new Path(inputFilePath);
        String hadoopKeyClass = IntWritable.class.getName();
        String hadoopValueClass = Text.class.getName();

        // Process file
        HDFSFileReader fileIterator = new HDFSSequenceFileReader(hostIp, hostPort, hadoopKeyClass,
                hadoopValueClass, path);
        Writable key = fileIterator.getInitKey();
        Writable value = fileIterator.getInitValue();
        while (fileIterator.readNext(key, value)) {
            System.out.println("KEY: " + key + " | VALUE: " + value);
        }
    }
}
