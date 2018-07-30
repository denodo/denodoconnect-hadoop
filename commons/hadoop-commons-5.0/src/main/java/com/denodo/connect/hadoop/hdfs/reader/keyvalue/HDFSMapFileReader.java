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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Class to iterate over a {@link MapFile}
 *
 * Note: due to the {@link MapFile} requirements, key must implement
 * {@link WritableComparable} as {@link Writable} is not enough
 *
 */
public class HDFSMapFileReader extends AbstractHDFSKeyValueFileReader {


    private MapFile.Reader currentReader;

    public HDFSMapFileReader(final Configuration configuration, final String hadoopKeyClass, final String hadoopValueClass,
            final Path outputPath, final String fileNamePattern, final String user) throws IOException, InterruptedException {

        super(configuration, hadoopKeyClass, hadoopValueClass, outputPath, fileNamePattern, user);
    }

    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
        final Configuration configuration) throws IOException {

        Path dirPath = path;
        if (isMapFile(path)) {
            dirPath = path.getParent();
            // A MapFile is a directory with two files 'data' and 'index':

        } else {
            throw new IllegalArgumentException("'" + path + "' is not a data file or an index file");
        }
        this.currentReader = new MapFile.Reader(dirPath, configuration);
    }

    private static boolean isMapFile(final Path path) {
        return MapFile.DATA_FILE_NAME.equals(path.getName()) || MapFile.INDEX_FILE_NAME.equals(path.getName());
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doRead(final K key, final V value) throws IOException {

        if (!(key instanceof WritableComparable)) {
            throw new UnsupportedOperationException("Key must be instance of WritableComparable to read from MapFile");
        }

        final WritableComparable<?> keyAsWC = (WritableComparable<?>) key;
        return this.currentReader.next(keyAsWC, value);
    }

    @Override
    public void closeReader() throws IOException {
        if (this.currentReader != null) {
            this.currentReader.close();
        }
    }

}
