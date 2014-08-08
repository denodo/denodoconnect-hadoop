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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;

/**
 * Class to iterate over a Delimited Text File.
 *
 */
public class HDFSDelimitedFileReader extends AbstractHDFSKeyValueFileReader {

    private String separator;
    private LineReader currentReader;
    private Text currentLine;


    public HDFSDelimitedFileReader(Configuration configuration, String separator,
        Path outputPath, String user) throws IOException, InterruptedException {

        super(configuration, Text.class.getName(), Text.class.getName(), outputPath, user);
        this.separator = separator;
        this.currentLine = new Text();
    }

    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        if (isFile(path)) {
            FSDataInputStream is = fileSystem.open(path);
            this.currentReader = new LineReader(is);
        } else {
            throw new IllegalArgumentException("'" + path + "' is not a file");
        }
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doRead(K key, V value) throws IOException {

        if (this.currentReader.readLine(this.currentLine) > 0) {
            String lineString = this.currentLine.toString();
            if (lineString.contains(this.separator)) {
                ((Text) key).set(StringUtils.substringBefore(lineString, this.separator));
                ((Text) value).set(StringUtils.substringAfter(lineString, this.separator));
                return true;
            }
            throw new IOException("Error reading line: line '" + lineString + "' does not contain the specified separator '"
                + this.separator + "'");
        }

        return false;
    }

    @Override
    public void closeReader() throws IOException {
        if (this.currentReader != null) {
            this.currentReader.close();
        }
    }

}
