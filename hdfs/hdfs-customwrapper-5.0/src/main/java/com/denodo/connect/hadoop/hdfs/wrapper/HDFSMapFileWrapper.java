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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.reader.HDFSKeyValueReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSMapFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS file custom Wrapper for reading map files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, file path,
 * Hadoop key class name and Hadoop value class name. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class HDFSMapFileWrapper extends AbstractHDFSFileWrapper {


    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_KEY_CLASS,
                "Hadoop Key class", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_VALUE_CLASS,
                "Hadoop Value class", true,
                CustomWrapperInputParameterTypeFactory.stringType())};


    public HDFSMapFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] getSpecificInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public HDFSKeyValueReader getHDFSFileReader(Map<String, String> inputValues) throws IOException {

        String fileSystemURI = inputValues.get(ParameterNaming.FILESYSTEM_URI);
        Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI);

        String hadoopKeyClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_KEY_CLASS);
        String hadoopValueClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_VALUE_CLASS);

        String inputFilePath = inputValues.get(ParameterNaming.INPUT_FILE_PATH);
        Path path = new Path(inputFilePath);

        return new HDFSMapFileReader(conf, hadoopKeyClass, hadoopValueClass,
            path);
    }

}
