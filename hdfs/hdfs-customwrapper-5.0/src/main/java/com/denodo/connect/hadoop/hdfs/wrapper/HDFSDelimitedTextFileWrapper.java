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
package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.HDFSDelimitedFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS file custom wrapper for reading delimited text files stored in HDFS
 * (Hadoop Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, file path
 * and file separator. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class HDFSDelimitedTextFileWrapper extends AbstractHDFSKeyValueFileWrapper {

    private static final  CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.SEPARATOR,
                "Separator of the delimited file(s) ", true,
                CustomWrapperInputParameterTypeFactory.stringType())
    };


    public HDFSDelimitedTextFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] doGetInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(super.doGetInputParameters(), INPUT_PARAMETERS);
    }

    @Override
    public HDFSFileReader getHDFSFileReader(Map<String, String> inputValues) throws IOException, InterruptedException {

        String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
        Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI);

        String separator = inputValues.get(Parameter.SEPARATOR);

        String inputFilePath = inputValues.get(Parameter.FILE_PATH);
        Path path = new Path(inputFilePath);

        return new HDFSDelimitedFileReader(conf, separator, path, null);
    }
}
