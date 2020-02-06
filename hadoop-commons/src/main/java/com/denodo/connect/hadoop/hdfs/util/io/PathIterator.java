/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2019, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.util.io;


import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PathIterator implements Iterator<Path> {

    private static final Logger LOG = LoggerFactory.getLogger(PathIterator.class);


    private RemoteIterator<LocatedFileStatus> fileIterator;
    private FileSystem fileSystem;
    private PathFilter fileFilter;

    private Path path;
    private Path cursor;

    public PathIterator(final Configuration conf, final Path path, final String finalNamePattern, final String user)
        throws IOException, InterruptedException {

        try {

            this.path = path;
            this.fileSystem = UserGroupInformation.isSecurityEnabled() ? FileSystem.get(conf) : FileSystem
                .get(FileSystem.getDefaultUri(conf), conf, user);

            initFileIterator();

            if (LOG.isDebugEnabled()) {
                LOG.debug("FileSystem is: " + this.fileSystem.getUri());
                LOG.debug("Path is: " + path);
            }

            this.fileFilter = new FileFilter(finalNamePattern);

        } catch (final IOException | RuntimeException | InterruptedException e) {
            close();

            throw e;
        }
    }

    @Override
    public boolean hasNext() {

        if (this.cursor != null) {
            return true;
        }

        try {

            boolean found = false;
            while (this.fileIterator.hasNext() && !found) {

                final FileStatus fileStatus = this.fileIterator.next();

                if (this.fileFilter.accept(fileStatus.getPath())) {
                    if (fileStatus.isFile()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Path of the file to read is: " + fileStatus.getPath());
                        }

                        this.cursor = fileStatus.getPath();
                    } else if (fileStatus.isSymlink()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Path of the symbolic link to read is: " + fileStatus.getSymlink());
                        }

                        this.cursor = fileStatus.getSymlink();
                    } else {
                        throw new IllegalArgumentException(
                            "'" + fileStatus.getPath() + "' is neither a file nor symbolic link");
                    }
                    found = true;
                }
            }

        } catch (final IOException e) {
            LOG.error("File Iterator error " + e.getMessage(), e);
            return  false;
        }

        return this.cursor != null;
    }

    @Override
    public Path next() {

        if (this.cursor == null) {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
        }

        final Path currentPath = this.cursor;
        this.cursor = null;

        return currentPath;
    }

    private void initFileIterator() throws IOException {

        this.fileIterator = this.fileSystem.listFiles(this.path, true);
        if (!this.fileIterator.hasNext()) {
            throw new IOException("'" + this.path + "' does not exist or it denotes an empty directory");
        }
    }

    public void close() throws IOException {

        if (this.fileSystem != null) {
            this.fileSystem.close();
            this.fileSystem = null;
        }
    }

    public void reset() throws IOException {

        initFileIterator();
        this.cursor = null;
    }

    public boolean isRootDirectory() throws IOException {
        return this.fileSystem.getFileStatus(this.path).isDirectory();
    }
}