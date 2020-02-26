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


import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.AUTOMATIC_PARALLELISM;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.CLUSTERING_FIELD;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.COLUMN_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.FILE_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.NOT_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARALLELISM_LEVEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.PARALLELISM_TYPE;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.ROW_PARALLEL;
import static com.denodo.connect.hadoop.hdfs.commons.naming.Parameter.THREADPOOL_SIZE;

import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

/**
 * Custom wrapper for reading Parquet files stored in Amazon S3. Ii is based on HDFSParquetFileWrapper, but it
 * simplifies S3 specific configuration.
 *
 */
public class S3ParquetFileWrapper extends HDFSParquetFileWrapper {

    
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =

        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. s3a://<bucket>",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.PARQUET_FILE_PATH,
                "Parquet File Path",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_NAME_PATTERN,
                "Regular expression to filter file names. Example: (.*)\\.parquet ", false,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include path column? ",
                false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(PARALLELISM_TYPE,
                "Type of parallelism, if any ",
                false, CustomWrapperInputParameterTypeFactory.enumStringType(
                new String[] {NOT_PARALLEL, AUTOMATIC_PARALLELISM, FILE_PARALLEL, ROW_PARALLEL, COLUMN_PARALLEL})),
            new CustomWrapperInputParameter(PARALLELISM_LEVEL,
                "Level of parallelism ",
                false, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(THREADPOOL_SIZE,
                "Number of threads in the pool",
                false, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(CLUSTERING_FIELD,
                "File/s sorted by this field ",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ACCESS_KEY_ID,
                "Access Key ID",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.SECRET_ACCESS_KEY,
                "Secret Access Key",
                false, CustomWrapperInputParameterTypeFactory.hiddenStringType()),
            new CustomWrapperInputParameter(Parameter.IAM_ROLE_ASSUME,
                "IAM Role to Assume",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ENDPOINT,
                "AWS S3 endpoint to connect to. Without this property, the standard region (s3.amazonaws.com) is assumed.",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.USE_EC2_IAM_CREDENTIALS,
                "Use EC2 IAM credentials ",
                false, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP}))
    };


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    protected Configuration getHadoopConfiguration(final Map<String, String> inputValues) throws CustomWrapperException {

        final String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);

        final CustomWrapperInputParameterValue coreSitePathValue = getInputParameterValue(Parameter.CORE_SITE_PATH);
        InputStream coreSiteIs = null;
        if (coreSitePathValue != null) {
            coreSiteIs = ((CustomWrapperInputParameterRouteValue) coreSitePathValue).getInputStream();
        }

        final CustomWrapperInputParameterValue hdfsSitePathValue = getInputParameterValue(Parameter.HDFS_SITE_PATH);
        InputStream hdfsSiteIs = null;
        if (hdfsSitePathValue != null) {
            hdfsSiteIs = ((CustomWrapperInputParameterRouteValue) hdfsSitePathValue).getInputStream();

        }

        final Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI, coreSiteIs, hdfsSiteIs);

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

        return conf;
    }


}
