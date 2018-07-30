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
package com.denodo.connect.hadoop.hdfs.util.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileFilter implements PathFilter {

    /* A _SUCCESS empty file is created on the successful completion of a MapReduce job; we should ignore this kind of files. */
    private static final String SUCCESS_FILE_NAME = "_SUCCESS";
    
    private String pattern;
    
    public FileFilter(final String pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean accept(final Path file) {

        return !SUCCESS_FILE_NAME.equals(file.getName()) && matchFilePattern(file.toUri().getPath());
    }

    private boolean matchFilePattern(final String filePath) {

        if (StringUtils.isNotBlank(this.pattern)) {
            return filePath.matches(this.pattern);
        }

        return true;
    }

}
