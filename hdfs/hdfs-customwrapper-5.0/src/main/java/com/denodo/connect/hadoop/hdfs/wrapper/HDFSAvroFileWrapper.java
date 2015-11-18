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
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSAvroFileReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.AvroSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS file custom wrapper for reading Avro files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, Avro
 * schema file path or Avro schema JSON. <br/>
 *
 */
public class HDFSAvroFileWrapper extends AbstractSecureHadoopWrapper {

    private static final Logger logger = Logger.getLogger(HDFSAvroFileWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_PATH,
                "Path to the Avro schema file. One of these parameters: '"
                    + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
                    false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_JSON,
                "JSON of the Avro schema. One of these parameters: '"
                    + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
                    false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING,
                "Delete the file after reading it? ", true,
                CustomWrapperInputParameterTypeFactory.booleanType(false))
        };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        try {

            boolean isSearchable = true;
            boolean isUpdateable = true;
            boolean isNullable = true;
            boolean isMandatory = true;

            CustomWrapperSchemaParameter avroFilePathParameter =
                new CustomWrapperSchemaParameter(Parameter.AVRO_FILE_PATH, Types.VARCHAR,
                    null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    !isUpdateable, !isNullable, isMandatory);

            String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
            Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI);
            Schema avroSchema = AvroSchemaUtils.buildSchema(inputValues, conf);
            SchemaElement javaSchema = HDFSAvroFileReader.getSchema(avroSchema);

            return new CustomWrapperSchemaParameter[] {avroFilePathParameter, VDPSchemaUtils.buildSchemaParameter(javaSchema)};

        } catch (Exception e) {
            logger.error("Error building wrapper schema", e);
            throw new CustomWrapperException(e.getMessage(), e);
        }

    }


    @Override
    public void doRun(CustomWrapperConditionHolder condition, List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues) throws CustomWrapperException {

        String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
        boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI);
        String avroFilePath = getAvroFilePath(condition);
        Path path = new Path(avroFilePath);

        HDFSFileReader reader = null;
        try {

            Schema avroSchema = AvroSchemaUtils.buildSchema(inputValues, conf);
            reader = new HDFSAvroFileReader(conf, path, avroSchema, null);
            Object[] rowData = new Object[2];
            rowData[0] = avroFilePath;
            Object avroData = reader.read();
            while (avroData != null && !isStopRequested()) {
                rowData[1] = avroData;
                result.addRow(rowData, projectedFields);

                avroData = reader.read();
            }

            if (delete) {
                reader.delete();
            }

        } catch (Exception e) {
            logger.error("Error accessing Avro file", e);
            throw new CustomWrapperException("Error accessing Avro file: " + e.getMessage(), e);

        } finally {
            try {
                if (reader != null && !delete) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error releasing the reader", e);
            }

        }
    }
    
    private static String getAvroFilePath(CustomWrapperConditionHolder condition) {

        String avroFilePath = null;

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        if (conditionMap != null) {
            for (Entry <CustomWrapperFieldExpression, Object> entry : conditionMap.entrySet()) {
                CustomWrapperFieldExpression field = entry.getKey();
                Object value = entry.getValue();
                if (field.getName().equals(Parameter.AVRO_FILE_PATH)) {
                    avroFilePath = (String) value;
                }
            }
        }

        return avroFilePath;
    }

}
