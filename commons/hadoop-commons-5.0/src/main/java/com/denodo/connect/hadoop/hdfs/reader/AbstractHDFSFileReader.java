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

import com.denodo.connect.hadoop.hdfs.util.io.FileFilter;

public abstract class AbstractHDFSFileReader implements HDFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(AbstractHDFSFileReader.class);
   

    private Configuration configuration;
    private Path outputPath;

    private FileSystem fileSystem;
    private RemoteIterator<LocatedFileStatus> fileIterator;
    private PathFilter fileFilter;
    private boolean firstReading;
    private Path currentPath;


    public AbstractHDFSFileReader(final Configuration configuration, final Path outputPath, final String fileNamePattern, final String user)
        throws IOException, InterruptedException {

        this.configuration = configuration;
        this.outputPath = outputPath;

        if (!UserGroupInformation.isSecurityEnabled()) {
            this.fileSystem = FileSystem.get(FileSystem.getDefaultUri(this.configuration), this.configuration, user);
        } else {
            this.fileSystem = FileSystem.get(this.configuration);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("FileSystem is: " + this.fileSystem.getUri());
            LOG.debug("Path is: " + outputPath);
        }

        this.fileFilter = new FileFilter(fileNamePattern);

        initFileIterator();
        
        this.firstReading = true;

    }

    public void initFileIterator() throws IOException {
        
        this.fileIterator = this.fileSystem.listFiles(this.outputPath, true);
        if (!this.fileIterator.hasNext()) {
            throw new IOException("'" + this.outputPath + "' does not exist or it denotes an empty directory");
        }
    }
    
    public Path nextFilePath() throws IOException {

        Path path = null;
        boolean found = false;
        while (this.fileIterator.hasNext() && !found) {
            
            final FileStatus fileStatus = this.fileIterator.next();
            
            if (this.fileFilter.accept(fileStatus.getPath())) {
                if (fileStatus.isFile()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Path of the file to read is: " + fileStatus.getPath());
                    }
                    
                    path = fileStatus.getPath();
                } else if (fileStatus.isSymlink()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Path of the symbolic link to read is: " + fileStatus.getSymlink());
                    }
                    
                    path = fileStatus.getSymlink();
                } else {
                    throw new IllegalArgumentException(
                            "'" + fileStatus.getPath() + "' is neither a file nor symbolic link");
                }
                found = true;
            }
        }
        
        if (path == null) {
            throw new NoSuchElementException();
        }

        return path;
    }
    
    public void openReader(final FileSystem fs, final Configuration conf) throws IOException {

        try {
            this.currentPath = nextFilePath();
            doOpenReader(fs, this.currentPath, conf);
        
        } catch (final NoSuchElementException e) {
            throw e;
        } catch (final IOException e) {
            throw new IOException("'" + this.currentPath + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            throw new RuntimeException("'" + this.currentPath + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }
    }
    

    @Override
    public Object read() throws IOException {
        
        try {

            if (isFirstReading()) {
                openReader(this.fileSystem, this.configuration);
                this.firstReading = false;
            }
            
            Object data = doRead();
            if (data != null) {
                return data;
            }
    
            // This reader does not have anything read -> take next one
            closeReader();
    
            // Take next file
            if (this.fileIterator.hasNext()) {
                openReader(this.fileSystem, this.configuration);
                data = doRead();
                if (data != null) {
                    return data;
                }
                closeReader();
                return read();
            }
    
            close();
            return null;

        } catch (final NoSuchElementException e) {
            return null;
        } catch (final IOException e) {
            throw new IOException("'" + this.currentPath + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            throw new RuntimeException("'" + this.currentPath + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }

    }
    
    private boolean isFirstReading() {
        return this.firstReading;
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

    public abstract void doOpenReader(FileSystem fs, Path path, Configuration conf) throws IOException;

    public abstract Object doRead() throws IOException;

    public abstract void closeReader() throws IOException;

}
