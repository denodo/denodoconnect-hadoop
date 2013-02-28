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
public class HDFSMapFileReader extends HDFSFileReader {


    private MapFile.Reader currentReader;

    public HDFSMapFileReader(String dataNodeIP, String dataNodePort,
        String hadoopKeyClass, String hadoopValueClass, Path outputPath) {

        super(dataNodeIP, dataNodePort, hadoopKeyClass, hadoopValueClass, outputPath);
    }

    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        Path dirPath = path;
        if (isFile(path)) {
            if (isMapFile(path)) {
                dirPath = path.getParent();
                // A MapFile is a directory with two files 'data' and 'index':
                // if path refer to one of these files we have to skip next file.
                nextFileIndex();
            } else {
                throw new IllegalArgumentException("'" + path + "' is not a data file or an index file");
            }
        }
        String dirName = dirPath.toUri().getPath();
        this.currentReader = new MapFile.Reader(fileSystem, dirName, configuration);
    }

    private static boolean isMapFile(Path path) {
        return MapFile.DATA_FILE_NAME.equals(path.getName()) || MapFile.INDEX_FILE_NAME.equals(path.getName());
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doReadNext(K key, V value) throws IOException {

        if (!(key instanceof WritableComparable)) {
            throw new UnsupportedOperationException("Key must be instance of WritableComparable to read from MapFile");
        }

        WritableComparable<?> keyAsWC = (WritableComparable<?>) key;
        return this.currentReader.next(keyAsWC, value);
    }

    @Override
    public void closeReader() throws IOException {
        this.currentReader.close();
    }

}
