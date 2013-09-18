/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.mapreduce.wrapper.handler;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.AbstractHDFSKeyValueFileReader;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.HDFSSequenceFileReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobFileReader;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * Handler to work with key/value files
 *
 */
public abstract class KeyValueFileOutputMapReduceJobHandler extends
    AbstractFileOutputMapReduceJobHandler {

    public static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
        new CustomWrapperInputParameter(Parameter.HADOOP_KEY_CLASS, "Hadoop key class ",
            false, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.HADOOP_VALUE_CLASS, "Hadoop value class ",
            false, CustomWrapperInputParameterTypeFactory.stringType())
    };

    @Override
    public Collection<SchemaElement> getSchema(Map<String, String> inputParameters) {

        String hadoopKeyClass = inputParameters.get(Parameter.HADOOP_KEY_CLASS);
        String hadoopValueClass = inputParameters.get(Parameter.HADOOP_VALUE_CLASS);

        return AbstractHDFSKeyValueFileReader.getSchema(hadoopKeyClass, hadoopValueClass);
    }


    @Override
    public MapReduceJobFileReader getOutputReader(Map<String, String> inputParameters) throws IOException {

        String hadoopKeyClass = inputParameters.get(Parameter.HADOOP_KEY_CLASS);
        String hadoopValueClass = inputParameters.get(Parameter.HADOOP_VALUE_CLASS);

        Configuration conf = getConfiguration(inputParameters);
        return new MapReduceJobFileReader(new HDFSSequenceFileReader(conf, hadoopKeyClass,
            hadoopValueClass, getOutputPath()));
    }

    @Override
    public void checkInput(Map<String, String> inputValues) {

        String hadoopKeyClass = inputValues.get(Parameter.HADOOP_KEY_CLASS);
        String hadoopValueClass = inputValues.get(Parameter.HADOOP_VALUE_CLASS);

        if (StringUtils.isBlank(hadoopKeyClass) || StringUtils.isBlank(hadoopValueClass)) {
            throw new IllegalArgumentException("'" + Parameter.HADOOP_KEY_CLASS + "' and '"
                + Parameter.HADOOP_VALUE_CLASS + "' are mandatory");
        }

    }

}
