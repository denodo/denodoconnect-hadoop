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


    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return DATA_SOURCE_INPUT_PARAMETERS;
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
