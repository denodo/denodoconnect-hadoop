/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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
package com.denodo.connect.dfs.wrapper.util.filesystem;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileSystemUtils {
    
    private static final  Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);
    
    private FileSystemUtils() {
        
    }

    /**
     * Modified from org.apache.hadoop.fs.FileSystem listFiles(Path f, boolean recursive))
     * to include statuses of directories.
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(final FileSystem fileSystem,
            final Path f, final boolean recursive) throws IOException {
          
        return new RemoteIterator<LocatedFileStatus>() {
            private LinkedList<RemoteIterator<LocatedFileStatus>> itors = new LinkedList<>();
            private RemoteIterator<LocatedFileStatus> curItor = fileSystem.listLocatedStatus(f);
            private LocatedFileStatus curFile;
            

            @Override
            public boolean hasNext() throws IOException {
                
                while (this.curFile == null) {
                    if (this.curItor.hasNext()) {
                        handleFileStat(this.curItor.next());
                    } else if (!this.itors.isEmpty()) {
                        this.curItor = this.itors.removeFirst();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            /**
             * Process the input stat.
             * If it is a file, return the file stat.
             * If it is a directory, return the dir stat and store the stat for later directory traversal if recursive is true;
             * ignore it if recursive is false.
             * @param stat input status
             * @throws IOException if any IO error occurs
             */
            private void handleFileStat(final LocatedFileStatus stat) throws IOException {
                
                this.curFile = stat;
                if (stat.isDirectory() && recursive) {
                    try {

                        final RemoteIterator<LocatedFileStatus> dirItor = fileSystem.listLocatedStatus(stat.getPath());
                        this.itors.add(dirItor);

                    } catch (final AccessControlException e) {
                        LOG.debug("Access to: '" + stat.getPath() + "' is denied", e);
                    }
                }
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                
                if (hasNext()) {
                    final LocatedFileStatus result = this.curFile;
                    this.curFile = null;
                    return result;
                }
                throw new java.util.NoSuchElementException("No more entry in " + f);
            }
          };
        }

}
