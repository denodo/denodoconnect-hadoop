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
import com.denodo.connect.hadoop.hdfs.util.schema.HDFSParquetSchemaReader;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderManager;
import com.denodo.connect.hadoop.hdfs.wrapper.concurrent.ReaderTask;
import com.denodo.vdb.engine.customwrapper.*;
import com.denodo.vdb.engine.customwrapper.condition.*;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.*;

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
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false))
        };

    private static final CustomWrapperInputParameter[] DATA_SOURCE_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. s3a://<bucket>",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ACCESS_KEY_ID,
                "Access Key ID",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.SECRET_ACCESS_KEY,
                "Secret Access Key",
                false, true, CustomWrapperInputParameterTypeFactory.hiddenStringType()),
            new CustomWrapperInputParameter(Parameter.IAM_ROLE_ASSUME,
                "IAM Role to Assume",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ENDPOINT,
                "AWS S3 endpoint to connect to. Without this property, the standard region (s3.amazonaws.com) is assumed.",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.USE_EC2_IAM_CREDENTIALS,
                "Use EC2 IAM credentials ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false, true, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP}))
    };

    private static final ReaderManager readerManager = ReaderManager.getInstance();
    private static int parallelism;

    static {
        parallelism = Runtime.getRuntime().availableProcessors() - 1;
        if (parallelism <= 0) {
            parallelism = 1;
        }
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return DATA_SOURCE_INPUT_PARAMETERS;
    }


        @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
            throws CustomWrapperException {

            HDFSParquetSchemaReader reader = null;
        try {

            final Configuration conf = getHadoopConfiguration(inputValues);

            if (inputValues.get(Parameter.USE_EC2_IAM_CREDENTIALS) != null && Boolean.parseBoolean(inputValues.get(Parameter.USE_EC2_IAM_CREDENTIALS))) {
                conf.set("fs.s3a.aws.credentials.provider", Parameter.INSTANCE_PROFILE_CREDENTIALS_PROVIDER);
            } else if (inputValues.get(Parameter.ACCESS_KEY_ID) != null && inputValues.get(Parameter.IAM_ROLE_ASSUME) != null) {
                conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
                conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));
                conf.set("fs.s3a.aws.credentials.provider", Parameter.AWS_ASSUMED_ROLE_PROVIDER);
                conf.set("fs.s3a.assumed.role.arn",inputValues.get(Parameter.IAM_ROLE_ASSUME));
            } else if (inputValues.get(Parameter.ACCESS_KEY_ID) != null && inputValues.get(Parameter.SECRET_ACCESS_KEY) != null) {
                conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
                conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));
            } else {
                conf.set("fs.s3a.aws.credentials.provider", Parameter.AWS_CREDENTIALS_PROVIDER);
            }

            if (inputValues.get(Parameter.ENDPOINT) != null) {
                conf.set("fs.s3a.endpoint", inputValues.get(Parameter.ENDPOINT));
            }

            final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
            final Path path = new Path(parquetFilePath);
            
            final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);

            final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

            reader = new HDFSParquetSchemaReader(conf, path, fileNamePattern, null, null, null);

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

        if (inputValues.get(Parameter.USE_EC2_IAM_CREDENTIALS) != null && Boolean.parseBoolean(inputValues.get(Parameter.USE_EC2_IAM_CREDENTIALS))) {
            conf.set("fs.s3a.aws.credentials.provider", Parameter.INSTANCE_PROFILE_CREDENTIALS_PROVIDER);
        } else if (inputValues.get(Parameter.ACCESS_KEY_ID) != null && inputValues.get(Parameter.IAM_ROLE_ASSUME) != null) {
            conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
            conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));
            conf.set("fs.s3a.aws.credentials.provider", Parameter.AWS_ASSUMED_ROLE_PROVIDER);
            conf.set("fs.s3a.assumed.role.arn",inputValues.get(Parameter.IAM_ROLE_ASSUME));
        } else if (inputValues.get(Parameter.ACCESS_KEY_ID) != null && inputValues.get(Parameter.SECRET_ACCESS_KEY) != null) {
            conf.set("fs.s3a.access.key", inputValues.get(Parameter.ACCESS_KEY_ID));
            conf.set("fs.s3a.secret.key", inputValues.get(Parameter.SECRET_ACCESS_KEY));
        } else {
            conf.set("fs.s3a.aws.credentials.provider", Parameter.AWS_CREDENTIALS_PROVIDER);
        }
        final String parquetFilePath = inputValues.get(Parameter.PARQUET_FILE_PATH);
        final Path path = new Path(parquetFilePath);
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));
        HDFSParquetSchemaReader schemaReader = null;
        try {
            schemaReader = new HDFSParquetSchemaReader(conf, path, fileNamePattern, null, projectedFields, condition);
            SchemaElement schema = null;
            if (schemaReader.getHasNullValueInConditions()) {
                schema = schemaReader.getSchema(conf);
            }
            FilterPredicate filterPredicate = ParquetSchemaUtils.buildFilter(condition.getComplexCondition(), schema);
            FilterCompat.Filter filter = null;
            if (filterPredicate != null) {
                ParquetInputFormat.setFilterPredicate(conf,filterPredicate);
                filter = ParquetInputFormat.getFilter(conf);
            }
            final Collection<ReaderTask> readers = new ArrayList<>(parallelism);
            try {
                Path currentPath = schemaReader.nextFilePath();
                while (currentPath != null) {
                    int i = 0;
                    while (currentPath != null && i < parallelism) {
                        final HDFSParquetFileReader currentReader = new HDFSParquetFileReader(conf, currentPath,
                            null, projectedFields, includePathColumn, filter,
                            schemaReader.getProjectedSchema(), schemaReader.getConditionFields());
                        readers.add(new ReaderTask(currentReader, projectedFields, result));
                        i++;
                        currentPath = schemaReader.nextFilePath();
                    }
                    readerManager.execute(readers);
                    readers.clear();
                }
            } catch (final NoSuchElementException e) {
                // throwed by nextFilePath
                readerManager.execute(readers);
            }
        } catch (final Exception e) {
            LOG.error("Error accessing Parquet file", e);
            throw new CustomWrapperException("Error accessing Parquet file: " + e.getMessage(), e);

        } finally {
            try {
                if (schemaReader != null ) {
                    schemaReader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the schemaReader", e);
            }
        }
    }

}
