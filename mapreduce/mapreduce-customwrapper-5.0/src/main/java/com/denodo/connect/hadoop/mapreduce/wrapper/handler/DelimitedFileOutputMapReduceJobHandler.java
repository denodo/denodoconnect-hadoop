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
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.HDFSDelimitedFileReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobFileReader;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * Handler to work with delimited files
 *
 */
public class DelimitedFileOutputMapReduceJobHandler extends
    AbstractFileOutputMapReduceJobHandler {

    public static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
        new CustomWrapperInputParameter(Parameter.SEPARATOR, "Separator of the delimited file(s)  ",
            false, CustomWrapperInputParameterTypeFactory.stringType())
    };


    @Override
    public Collection<SchemaElement> getSchema(Map<String, String> inputParameters) {

        String hadoopKeyClass = inputParameters.get(Parameter.HADOOP_KEY_CLASS);
        String hadoopValueClass = inputParameters.get(Parameter.HADOOP_VALUE_CLASS);

        return AbstractHDFSKeyValueFileReader.getSchema(hadoopKeyClass, hadoopValueClass);
    }

    @Override
    public MapReduceJobFileReader getOutputReader(Map<String, String> inputParameters) throws IOException, InterruptedException {

        Configuration conf = getConfiguration(inputParameters);

        String separator = inputParameters.get(Parameter.SEPARATOR);
        String user = inputParameters.get(Parameter.USER);

        return new MapReduceJobFileReader(new HDFSDelimitedFileReader(conf, separator, getOutputPath(), user));
    }

    @Override
    public void checkInput(Map<String, String> inputValues) {

        String separator = inputValues.get(Parameter.SEPARATOR);

        if (StringUtils.isEmpty(separator)) {
            throw new IllegalArgumentException("'" + Parameter.SEPARATOR + "' is mandatory");
        }
    }

}
