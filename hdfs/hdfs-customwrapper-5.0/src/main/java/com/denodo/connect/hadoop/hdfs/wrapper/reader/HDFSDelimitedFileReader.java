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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;

import com.denodo.connect.hadoop.hdfs.wrapper.commons.exception.InternalErrorException;

/**
 * Class to iterate over a Delimited Text File.
 *
 */
public class HDFSDelimitedFileReader extends HDFSFileReader {


    private String separator;
    private FSDataInputStream is;
    private LineReader currentReader;
    private Text currentLine;


    public HDFSDelimitedFileReader(String dataNodeIP, String dataNodePort,
        String separator, Path outputPath) {

        super(dataNodeIP, dataNodePort, Text.class.getName(), Text.class.getName(), outputPath);

        this.separator = separator;
        this.currentLine = new Text();
    }

    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        if (isFile(path)) {
            this.is = fileSystem.open(path);
            this.currentReader = new LineReader(this.is);
        } else {
            throw new IllegalArgumentException("'" + path + "' is not a file");
        }
    }

    @Override
    public <K extends Writable, V extends Writable> boolean doReadNext(K key, V value) throws IOException {

        if (this.currentReader.readLine(this.currentLine) > 0) {
            if (this.currentLine.toString().contains(this.separator)) {
                ((Text) key).set(new Text(StringUtils.substringBefore(this.currentLine.toString(), this.separator)));
                ((Text) value).set(new Text(StringUtils.substringAfter(this.currentLine.toString(), this.separator)));
                // Has next -> Values are in key and value -> do anything else
                return true;
            }
            throw new InternalErrorException(String.format(
                "Error reading line: line does not contain the specified separator '%s' ",
                this.separator));
        }

        return false;
    }

    @Override
    public void closeReader() throws IOException {
        if (this.is != null) {
            this.is.close();
            this.currentReader.close();
        }
    }

}
