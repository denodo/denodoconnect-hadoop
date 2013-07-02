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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.reader.keyvalue.AbstractHDFSKeyValueFileReader;

public abstract class AbstractHDFSFileReader implements HDFSFileReader {

    private static final Logger logger = Logger.getLogger(AbstractHDFSKeyValueFileReader.class);

    private Configuration configuration;
    private Path outputPath;

    private FileSystem fileSystem;
    private FileStatus[] fss;
    private int currentFileIndex;


    public AbstractHDFSFileReader(Configuration configuration, Path outputPath)
        throws IOException {

        this.configuration = configuration;
        this.outputPath = outputPath;
        this.currentFileIndex = -1;

        this.fileSystem = FileSystem.get(this.configuration);

        if (logger.isDebugEnabled()) {
            logger.debug("FileSystem is: " + this.fileSystem.getUri());
            logger.debug("Path is: " + outputPath);
        }

        this.fss = this.fileSystem.listStatus(outputPath);
        if (ArrayUtils.isEmpty(this.fss)) {
            throw new IOException("'" + outputPath + "' does not exist or it denotes an empty directory");
        }

    }

    @Override
    public Object read() throws IOException {

        if (isFirstReading()) {
            nextFileIndex();
            openReader(this.fileSystem, this.fss[this.currentFileIndex].getPath(),
                this.configuration);
        }

        Object data = doRead();
        if (data != null) {
            return data;
        }

        // This reader does not have anything read -> take next one
        closeReader();

        // Take next file status reader
        nextFileIndex();
        if (this.fss.length > this.currentFileIndex) {
            openReader(this.fileSystem, this.fss[this.currentFileIndex].getPath(),
                this.configuration);
            data = doRead();
            if (data != null) {
                return data;
            }
            return read();
        }

        close();
        return null;

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

    @Override
    public void close() throws IOException {
        closeReader();
        if (this.fileSystem != null) {
            this.fileSystem.close();
            this.fileSystem = null;
        }
    }

    @Override
    public void delete() throws IOException {

        if (this.fileSystem == null) {
            this.fileSystem = FileSystem.get(this.configuration);
        }
        this.fileSystem.delete(this.outputPath, true);
        this.fileSystem.close();
        this.fileSystem = null;
    }

    public abstract void openReader(FileSystem fs, Path path, Configuration conf) throws IOException;

    public abstract Object doRead() throws IOException;

    public abstract void closeReader() throws IOException;

}
