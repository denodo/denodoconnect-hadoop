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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

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
    private CompressionCodecFactory codecFactory;


    public HDFSDelimitedFileReader(final Configuration configuration, final CSVConfig cvsConfig,
        final Path outputPath, final String fileNamePattern, final String user, final boolean includePathColumn) throws IOException, InterruptedException {

        super(configuration, outputPath, fileNamePattern, user, includePathColumn);
        this.csvConfig = cvsConfig;
        this.linesToSkip = cvsConfig.isHeader() ? 1 : 0;
        this.linesSkipped = 0;
        this.codecFactory = new CompressionCodecFactory(configuration);
    }
    
    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
        final Configuration configuration) throws IOException {

        InputStream is = fileSystem.open(path);
        final CompressionCodec codec = this.codecFactory.getCodec(path);
        if (codec != null) {
            is = codec.createInputStream(is); // for reading compressed files
        }
        this.reader = new CSVReader(new InputStreamReader(new BOMInputStream(is)), this.csvConfig);
    }
    
    @Override
    public Object doRead() {

        skipLines();
        if (this.reader.hasNext()) {
            final List<String> data = this.reader.next();
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
    public void closeReader() {
        if (this.reader != null) {
            this.reader.close();
        }
    }

}
