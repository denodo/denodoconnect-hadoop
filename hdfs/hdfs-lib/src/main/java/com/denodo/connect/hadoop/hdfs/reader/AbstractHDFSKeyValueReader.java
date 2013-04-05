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
package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.util.configuration.InitUtils;

public abstract class AbstractHDFSKeyValueReader implements HDFSKeyValueReader {

    private static final Logger logger = Logger.getLogger(AbstractHDFSKeyValueReader.class);

    private String hadoopKeyClass;
    private String hadoopValueClass;
    private Configuration configuration;

    private FileSystem fileSystem;
    private FileStatus[] fss;
    private int currentFileIndex;


    public AbstractHDFSKeyValueReader(Configuration configuration,
        String hadoopKeyClass, String hadoopValueClass, Path outputPath)
        throws IOException {

        this.configuration = configuration;
        this.hadoopKeyClass = hadoopKeyClass;
        this.hadoopValueClass = hadoopValueClass;
        this.currentFileIndex = -1;

        this.fileSystem = FileSystem.get(this.configuration);

        if (logger.isDebugEnabled()) {
            logger.debug("FileSystem is: " + this.fileSystem.getUri());
            logger.debug("Path is: " + outputPath);
        }

        this.fss = this.fileSystem.listStatus(outputPath);
        if (this.fss == null) {
            throw new IOException("'" + outputPath + "' does not exist");
        }
    }

    /**
     * Reads the next key-value pair and stores it in the key and value parameters.
     * This method is in charge of reading every output file and closing them in a
     * transparent way.
     */
    @Override
    public boolean readNext(Writable key, Writable value) throws IOException {

        if (this.fss == null || this.fss.length == 0) {
            return false;
        }

        if (isFirstReading()) {
            nextFileIndex();
            openReader(this.fileSystem, this.fss[this.currentFileIndex].getPath(),
                this.configuration);
        }

        if (doReadNext(key, value)) {
            // 'key' and 'value' are filled with their values
            return true;
        }

        // This reader does not have anything read -> take next one
        closeReader();

        // Take next file status reader
        nextFileIndex();
        if (this.fss.length > this.currentFileIndex) {
            openReader(this.fileSystem, this.fss[this.currentFileIndex].getPath(),
                this.configuration);
            if (doReadNext(key, value)) {
                // Has next -> 'key' and 'value' are filled with their values
                return true;
            }
            return readNext(key, value);
        }

        close();
        return false;

    }

    private boolean isFirstReading() {
        return this.currentFileIndex < 0;
    }

    public void nextFileIndex() {
        this.currentFileIndex++;
    }

    public boolean isFile(Path path) throws IOException {
        return this.fileSystem.isFile(path);
    }

    /**
     * @return an instance of the key class initialized (necessary
     * to read output).
     */
    @Override
    public Writable getInitKey() {
        return InitUtils.getInitKey(this.hadoopKeyClass, this.configuration);
    }

    /**
     * @return an instance of the value class initialized (necessary
     * to read output).
     */
    @Override
    public Writable getInitValue() {
        return InitUtils.getInitValue(this.hadoopValueClass, this.configuration);
    }

    @Override
    public void close() throws IOException {
        closeReader();
        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }

    public abstract void openReader(FileSystem fs, Path path,
        Configuration conf) throws IOException;

    public abstract <K extends Writable, V extends Writable> boolean doReadNext(
        K key, V value) throws IOException;

    public abstract void closeReader() throws IOException;

}
