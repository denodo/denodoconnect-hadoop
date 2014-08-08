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
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.HDFSMapFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;
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
public class HDFSMapFileWrapper extends AbstractHDFSKeyValueFileWrapper {

    private static final  CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.HADOOP_KEY_CLASS,
                "Hadoop key class", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.HADOOP_VALUE_CLASS,
                "Hadoop value class", true,
                 CustomWrapperInputParameterTypeFactory.stringType())
        };

    public HDFSMapFileWrapper() {
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

        String hadoopKeyClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_KEY_CLASS));
        String hadoopValueClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_VALUE_CLASS));

        String inputFilePath = inputValues.get(Parameter.FILE_PATH);
        Path path = new Path(inputFilePath);

        return new HDFSMapFileReader(conf, hadoopKeyClass, hadoopValueClass, path, null);
    }

}
