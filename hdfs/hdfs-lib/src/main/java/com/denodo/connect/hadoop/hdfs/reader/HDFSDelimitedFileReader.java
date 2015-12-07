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
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.util.csv.CSVConfig;
import com.denodo.connect.hadoop.hdfs.util.csv.CSVReader;

/**
 * Class to iterate over a Delimited Text File.
 *
 */
public class HDFSDelimitedFileReader extends AbstractHDFSFileReader {

    private CSVConfig csvConfig;
    private int linesToSkip;
    private int linesSkipped;
    private CSVReader reader;


    public HDFSDelimitedFileReader(Configuration configuration, CSVConfig cvsConfig,
        Path outputPath, String user) throws IOException, InterruptedException {

        super(configuration, outputPath, user);
        this.csvConfig = cvsConfig;
        this.linesToSkip = cvsConfig.isHeader() ? 1 : 0;
        this.linesSkipped = 0;
    }
    
    @Override
    public void openReader(FileSystem fileSystem, Path path,
        Configuration configuration) throws IOException {

        if (isFile(path)) {
            FSDataInputStream is = fileSystem.open(path);
            this.reader = new CSVReader(new InputStreamReader(is), this.csvConfig);
        } else {
            throw new IllegalArgumentException("'" + path + "' is not a file");
        }
    }
    
    @Override
    public Object doRead() {

        skipLines();
        if (this.reader.hasNext()) {
            List<String> data = this.reader.next();
            return data.toArray();
        }

        this.linesSkipped = 0;
        return null;
    }
    
    private void skipLines() {
        
        while (this.linesSkipped < this.linesToSkip && this.reader.hasNext()) {
            this.reader.next();
            this.linesSkipped ++;
        }
    }


    @Override
    public void closeReader() throws IOException {
        if (this.reader != null) {
            this.reader.close();
        }
    }

}
