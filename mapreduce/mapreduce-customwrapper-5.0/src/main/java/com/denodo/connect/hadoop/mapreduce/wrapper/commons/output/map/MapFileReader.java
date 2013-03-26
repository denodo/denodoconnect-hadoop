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
package com.denodo.connect.hadoop.mapreduce.wrapper.commons.output.map;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.denodo.connect.hadoop.hdfs.reader.HDFSKeyValueReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSMapFileReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.output.IMapReduceTaskOutputReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.util.MapReduceUtils;

/**
 * Class to iterate over a {@link MapFile}
 *
 * Note: due to the {@link MapFile} requirements, key must implement
 * {@link WritableComparable} as {@link Writable} is not enough
 *
 */
public class MapFileReader implements IMapReduceTaskOutputReader {

    private HDFSKeyValueReader reader;
    private String dataNodeIP;
    private String dataNodePort;
    private Path outputPath;
    private boolean deleteOutputOnFinish;

    public MapFileReader(String dataNodeIP, String dataNodePort,
        String hadoopKeyClass, String hadoopValueClass, Path outputPath,
        boolean deleteOutputOnFinish) throws IOException {

        this.reader = new HDFSMapFileReader(dataNodeIP, dataNodePort, hadoopKeyClass,
            hadoopValueClass, outputPath);
        this.dataNodeIP = dataNodeIP;
        this.dataNodePort = dataNodePort;
        this.outputPath = outputPath;
        this.deleteOutputOnFinish = deleteOutputOnFinish;
    }

    @Override
    public boolean readNext(Writable key, Writable value) throws IOException {
        return this.reader.readNext(key, value);
    }

    @Override
    public Writable getInitKey() {
        return this.reader.getInitKey();
    }

    @Override
    public Writable getInitValue() {
        return this.reader.getInitValue();
    }

    @Override
    public void close() throws IOException {

        if (this.reader != null) {
            this.reader.close();
        }
        if (this.deleteOutputOnFinish) {
            MapReduceUtils.deleteFile(this.dataNodeIP, this.dataNodePort, this.outputPath);
        }
    }


}
