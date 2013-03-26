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

import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.reader.HDFSKeyValueReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSSequenceFileReader;
import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS file custom Wrapper for reading sequence files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: NameNode IP, NameNode port, file path,
 * Hadoop key class and Hadoop value class. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class HDFSSequenceFileWrapper extends AbstractHDFSFileWrapper {

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_KEY_CLASS,
                "Hadoop Key class", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_VALUE_CLASS,
                "Hadoop Value class", true,
                CustomWrapperInputParameterTypeFactory.stringType())};


    public HDFSSequenceFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] getSpecificInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public HDFSKeyValueReader getHDFSFileReader(Map<String, String> inputValues) throws IOException {

        String dataNodeIP = inputValues.get(ParameterNaming.HOST_IP);
        String dataNodePort = inputValues.get(ParameterNaming.HOST_PORT);
        String hadoopKeyClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_KEY_CLASS);
        String hadoopValueClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_VALUE_CLASS);
        String inputFilePath = inputValues.get(ParameterNaming.INPUT_FILE_PATH);
        Path path = new Path(inputFilePath);

        return new HDFSSequenceFileReader(dataNodeIP, dataNodePort, hadoopKeyClass,
            hadoopValueClass, path);
    }

}
