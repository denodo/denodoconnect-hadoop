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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.wrapper.util.http.HTTPUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.util.http.URIUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS file custom wrapper for reading key/value delimited text files stored in
 * HDFS (Hadoop Distributed File System) using WebHDFS (HTTP REST Access to HDFS).
 * <ul>
 * Supported operations:
 * <li>OPEN</li>
 * <li>DELETE</li>
 * </ul>
 * Key/value pairs contained in the file will be returned by the wrapper.
 */
public class WebHDFSFileWrapper extends AbstractCustomWrapper {

    private static Logger logger = Logger.getLogger(WebHDFSFileWrapper.class);


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.HOST_IP, "Host IP or <bucket>.s3.amazonaws.com for Amazon S3 ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.HOST_PORT, "HTTP port: default port for WebHDFS is 50075. For HttpFS is 14000. ",
                true, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(Parameter.USER, "User that will perform the operation or <id>:<secret> for Amazon S3 ",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_PATH, "Absolute file path ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.SEPARATOR, "Separator of the delimited file ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING, "Delete the file after reading it? ",
                true, CustomWrapperInputParameterTypeFactory.booleanType(false))
        };
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        return new CustomWrapperSchemaParameter[] {
            new CustomWrapperSchemaParameter(Parameter.KEY, Types.VARCHAR,
                null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, isNullable, !isMandatory),
            new CustomWrapperSchemaParameter(Parameter.VALUE, Types.VARCHAR,
                null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, isNullable, !isMandatory)
        };
    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {


        String host = inputValues.get(Parameter.HOST_IP);
        int port = Integer.parseInt(inputValues.get(Parameter.HOST_PORT));
        String user = inputValues.get(Parameter.USER);
        String filePath = inputValues.get(Parameter.FILE_PATH);
        filePath = normalizePath(filePath);
        String separator = inputValues.get(Parameter.SEPARATOR);
        boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        DefaultHttpClient httpClient = new DefaultHttpClient();
        BufferedReader br = null;
        try {

            URI openURI = URIUtils.getWebHDFSOpenURI(host, port, user, filePath);
            InputStream is = HTTPUtils.requestGet(openURI, httpClient);
            br = new BufferedReader(new InputStreamReader(is));
            String line = null;
            String[] asArray = null;
            while ((line = br.readLine()) != null) {
                asArray = line.split(separator);
                if (asArray.length != 2) {
                    throw new IOException("Error reading file: line '" + line
                        + "' does not contain the separator '" + separator + "'");
                }
                result.addRow(asArray, projectedFields);
            }

            if (delete) {
                try {
                    URI deleteURI = URIUtils.getWebHDFSDeleteURI(host, port, user, filePath);
                    HTTPUtils.requestDelete(deleteURI, httpClient);
                } catch (Exception e) {
                    logger.error("Error deleting the file", e);
                }
            }

        } catch (Exception e) {
            logger.error("Error accessing WebHDFS", e);
            throw new CustomWrapperException("Error accessing WebHDFS: " + e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(br);

            httpClient.getConnectionManager().shutdown();

        }
    }

    private static String normalizePath(String filePath) {

        if (!filePath.startsWith("/")) {
            return "/" + filePath;
        }

        return filePath;
    }

}
