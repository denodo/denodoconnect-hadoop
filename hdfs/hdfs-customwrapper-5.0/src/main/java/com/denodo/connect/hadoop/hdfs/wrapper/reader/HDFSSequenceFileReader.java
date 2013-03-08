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
package com.denodo.connect.hadoop.hdfs.wrapper.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;


/**
 * Class to iterate over a {@link SequenceFile}
 *
 */
public class HDFSSequenceFileReader extends HDFSFileReader {


    private SequenceFile.Reader currentReader;

    public HDFSSequenceFileReader(String dataNodeIP, String dataNodePort,
        String hadoopKeyClass, String hadoopValueClass, Path outputPath) {

        super(dataNodeIP, dataNodePort, hadoopKeyClass, hadoopValueClass, outputPath);
    }

    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        if (isFile(path)) {
            this.currentReader = new SequenceFile.Reader(fileSystem, path, configuration);
        } else {
            throw new IllegalArgumentException("'" + path + "' is not a file");
        }
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doReadNext(K key, V value) throws IOException {
        return this.currentReader.next(key, value);
    }

    @Override
    public void closeReader() throws IOException {
        if (this.currentReader != null) {
            this.currentReader.close();
        }
    }

}
