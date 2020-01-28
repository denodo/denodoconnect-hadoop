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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSAvroFileReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
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
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;

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

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSAvroFileWrapper.class); 

    
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_PATH,
                "Path to the Avro schema file. One of these parameters: '"
                    + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
                    false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.AVRO_SCHEMA_JSON,
                "JSON of the Avro schema. One of these parameters: '"
                    + Parameter.AVRO_SCHEMA_PATH + "' or '" + Parameter.AVRO_SCHEMA_JSON + "' must be specified",
                    false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_NAME_PATTERN,
                    "Regular expression to filter file names. Example: (.*)\\.avro ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),               
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING,
                "Delete the file after reading it? ",
                true, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false))
        };

    private static final CustomWrapperInputParameter[] DATA_SOURCE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP}))
        };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, super.getInputParameters());
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(DATA_SOURCE_INPUT_PARAMETERS, super.getDataSourceInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        try {

            final boolean isSearchable = true;
            final boolean isUpdateable = true;
            final boolean isNullable = true;
            final boolean isMandatory = true;

            final CustomWrapperSchemaParameter avroFilePathParameter =
                new CustomWrapperSchemaParameter(Parameter.AVRO_FILE_PATH, Types.VARCHAR,
                    null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    !isUpdateable, !isNullable, isMandatory);

            final Configuration conf = getHadoopConfiguration(inputValues);
            final Schema avroSchema = AvroSchemaUtils.buildSchema(inputValues, conf);
            final SchemaElement javaSchema = HDFSAvroFileReader.getSchema(avroSchema);
            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));
            if(includePathColumn){
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, isNullable, !isMandatory);
                return new CustomWrapperSchemaParameter[] {avroFilePathParameter, VDPSchemaUtils.buildSchemaParameter(javaSchema), filePath};
            }
            return new CustomWrapperSchemaParameter[] {avroFilePathParameter, VDPSchemaUtils.buildSchemaParameter(javaSchema)};

        } catch (final Exception e) {
            LOG.error("Error building wrapper schema", e);
            throw new CustomWrapperException(e.getMessage(), e);
        }

    }

    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        final Configuration conf = getHadoopConfiguration(inputValues);

        final boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        final String avroFilePath = getAvroFilePath(condition);
        final Path path = new Path(avroFilePath);
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        HDFSFileReader reader = null;
        try {

            final Schema avroSchema = AvroSchemaUtils.buildSchema(inputValues, conf);
            reader = new HDFSAvroFileReader(conf, path, fileNamePattern, avroSchema, null, includePathColumn);
            final Object[] rowData;
            if(includePathColumn){
                rowData = new Object[3];
            }else{
                rowData = new Object[2];
            }

            rowData[0] = avroFilePath;
            Object avroData = reader.read();
            while (avroData != null && !isStopRequested()) {
                if(includePathColumn){
                    final Object[] avroArray = (Object[]) avroData;
                    rowData[1]= ArrayUtils.subarray(avroArray,0,avroArray.length-1);
                    rowData[2]= avroArray[avroArray.length-1];
                }else {
                    rowData[1] = avroData;
                }
                result.addRow(rowData, projectedFields);

                avroData = reader.read();
            }

            if (delete) {
                reader.delete();
            }

        } catch (final Exception e) {
            LOG.error("Error accessing Avro file", e);
            throw new CustomWrapperException("Error accessing Avro file: " + e.getMessage(), e);

        } finally {
            try {
                if (reader != null && !delete) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }

        }
    }

    private static String getAvroFilePath(final CustomWrapperConditionHolder condition) {

        String avroFilePath = null;

        final Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        if (conditionMap != null) {
            for (final Entry <CustomWrapperFieldExpression, Object> entry : conditionMap.entrySet()) {
                final CustomWrapperFieldExpression field = entry.getKey();
                final Object value = entry.getValue();
                if (field.getName().equals(Parameter.AVRO_FILE_PATH)) {
                    avroFilePath = (String) value;
                }
            }
        }

        return avroFilePath;
    }

}
