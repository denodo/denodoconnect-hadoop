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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSAvroFileReader;
import com.denodo.connect.hadoop.hdfs.util.schema.AvroSchemaUtils;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobAvroFileReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobFileReader;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * Handler to work with Avro files
 *
 */
public class AvroFileOutputMapReduceJobHandler extends
    AbstractFileOutputMapReduceJobHandler {

    public static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
        new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_PATH,
            "Path to the Avro schema file. One of these parameters: '"
                + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
            false, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_JSON,
            "JSON of the Avro schema. One of these parameters: '"
                + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
            false, CustomWrapperInputParameterTypeFactory.stringType())
    };

    @Override
    public Collection<SchemaElement> getSchema(Map<String, String> inputParameters) {

        try {
            Configuration conf = getConfiguration(inputParameters);
            Schema schema = AvroSchemaUtils.buildSchema(inputParameters, conf);
            return Arrays.asList(HDFSAvroFileReader.getSchema(schema));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public MapReduceJobFileReader getOutputReader(Map<String, String> inputParameters) throws IOException {

        Configuration conf = getConfiguration(inputParameters);
        Schema schema = AvroSchemaUtils.buildSchema(inputParameters, conf);

        return new MapReduceJobAvroFileReader(new HDFSAvroFileReader(conf, getOutputPath(), schema));
    }

    @Override
    public void checkInput(Map<String, String> inputValues) {

        String schemaFile = inputValues.get(Parameter.AVRO_SCHEMA_PATH);
        String schemaJSON = inputValues.get(Parameter.AVRO_SCHEMA_JSON);

        if (StringUtils.isBlank(schemaFile) && StringUtils.isBlank(schemaJSON)) {
            throw new IllegalArgumentException("One of these parameters: '"
                + Parameter.AVRO_SCHEMA_JSON + "' or '" + Parameter.AVRO_SCHEMA_PATH + "' must be specified");
        }

    }

}
