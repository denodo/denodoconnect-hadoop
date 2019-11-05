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


import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.vdb.engine.customwrapper.*;
import com.denodo.vdb.engine.customwrapper.condition.*;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.*;

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.*;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

/**
 * HDFS file custom wrapper for reading Parquet files stored in HDFS (Hadoop
 * Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, Avro
 * schema file path or Avro schema JSON. <br/>
 *
 */
public class S3ParquetFileWrapper extends HDFSParquetFileWrapper {

    private static final  Logger LOG = LoggerFactory.getLogger(S3ParquetFileWrapper.class);

    
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\\\@<bucket>t ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ACCESS_KEY_ID,
                "Access Key ID",
                true,  CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.SECRET_ACCESS_KEY,
                "Secret Access Key",
                true,  CustomWrapperInputParameterTypeFactory.hiddenStringType()),
            new CustomWrapperInputParameter(Parameter.ENDPOINT,
                "AWS S3 endpoint to connect to. Without this property, the standard region (s3.amazonaws.com) is assumed.",
                false,  CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ", false,
                CustomWrapperInputParameterTypeFactory.booleanType(false))
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
            throws CustomWrapperException {

        HDFSParquetFileReader reader = null;
        try {

            final Configuration conf = getHadoopConfiguration(inputValues);

            conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
            conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));
            if (inputValues.get(Parameter.ENDPOINT) != null) {
                conf.set("fs.s3a.endpoint", inputValues.get(Parameter.ENDPOINT));
            }

            final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
            final Path path = new Path(parquetFilePath);
            
            final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

            reader = new HDFSParquetFileReader(conf, path, fileNamePattern, null, null, null, includePathColumn, true, null);

            final SchemaElement javaSchema = reader.getSchema(conf);
            if(includePathColumn){
                final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, false,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
                return (CustomWrapperSchemaParameter[]) ArrayUtils.add(VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements()),filePath);
            }else {
                return VDPSchemaUtils.buildSchemaParameterParquet(javaSchema.getElements());
            }
        } catch (final NoSuchElementException e) {
            throw new CustomWrapperException("There are no files in " + inputValues.get(Parameter.PARQUET_FILE_PATH) 
            + (StringUtils.isNotBlank(inputValues.get(Parameter.FILE_NAME_PATTERN)) 
                ? " matching the provided file pattern: " + inputValues.get(Parameter.FILE_NAME_PATTERN)
                : ""));
        } catch (final Exception e) {
            LOG.error("Error building wrapper schema", e);
            throw new CustomWrapperException(e.getMessage(), e);
        } finally {
            try {
                if (reader != null ) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }
        }
    }

    @Override
    public void doRun(final CustomWrapperConditionHolder condition, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues) throws CustomWrapperException {

        final Configuration conf = getHadoopConfiguration(inputValues);

        conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
        conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));

        final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
        final Path path = new Path(parquetFilePath);
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));
        HDFSParquetFileReader reader = null;
        try {
            reader = new HDFSParquetFileReader(conf, path, fileNamePattern, null, projectedFields, condition, includePathColumn, false, null);
            SchemaElement schema = null;
            if (reader.getHasNullValueInConditions()) {
                schema = reader.getSchema(conf);
            }
            FilterPredicate filterPredicate = reader.buildFilter(condition.getComplexCondition(), schema);
            FilterCompat.Filter filter = null;
            if (filterPredicate != null) {
                ParquetInputFormat.setFilterPredicate(conf,filterPredicate);
                filter = ParquetInputFormat.getFilter(conf);
            }
            reader.setFilter(filter);
            int conditionSize = reader.getConditionFields().size();
            LOG.trace("We get the condition fields (only for simple fields) excluding the repeated " +
                "projected fields and get size to delete the last elements of the data array");
            Object parquetData = reader.read();
            while (parquetData != null && !isStopRequested()) {
                result.addRow( (Object[])parquetData, projectedFields);
                parquetData = reader.read();
            }
        } catch (final Exception e) {
            LOG.error("Error accessing Parquet file", e);
            throw new CustomWrapperException("Error accessing Parquet file: " + e.getMessage(), e);

        } finally {
            try {
                if (reader != null ) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }
        }
    }

}
