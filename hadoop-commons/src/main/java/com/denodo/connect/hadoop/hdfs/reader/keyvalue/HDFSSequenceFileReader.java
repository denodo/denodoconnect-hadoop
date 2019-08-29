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
package com.denodo.connect.hadoop.hdfs.reader.keyvalue;

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
public class HDFSSequenceFileReader extends AbstractHDFSKeyValueFileReader {


    private SequenceFile.Reader currentReader;

    public HDFSSequenceFileReader(final Configuration configuration, final String hadoopKeyClass,
            final String hadoopValueClass, final Path outputPath, final String fileNamePattern, final String user, final boolean includePathColumn)
            throws IOException, InterruptedException {

        super(configuration, hadoopKeyClass, hadoopValueClass, outputPath, fileNamePattern, user, includePathColumn);
    }

    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
        final Configuration configuration) throws IOException {

        this.currentReader = new SequenceFile.Reader(fileSystem, path, configuration);
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doRead(final K key, final V value) throws IOException {
        return this.currentReader.next(key, value);
    }

    @Override
    public void closeReader() throws IOException {
        if (this.currentReader != null) {
            this.currentReader.close();
        }
    }

}
