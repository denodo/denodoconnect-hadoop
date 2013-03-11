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
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.wrapper.commons.exception.InternalErrorException;
import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
import com.denodo.util.net.URLUtil;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

/**
 * HDFS file custom wrapper for reading key/value delimited text files stored in
 * HDFS (Hadoop Distributed File System) using the HttpFs Gateway and REST API
 * <ul>
 * Supported operations:
 * <li>OPEN</li>
 * </ul>
 * Key/value pairs contained in the file will be returned by the wrapper.
 */
public class HttpFsFileWrapper extends AbstractCustomWrapper {

    /**
     * Logger for this class
     */
    private static Logger logger = Logger.getLogger(HttpFsFileWrapper.class);


    private static final String REST_API_PREFIX = "/webhdfs/v1";


    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HOST_IP,
                "HttpFS IP ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PORT,
                "HttpFS port, default for HttpFS is 50075 for HttpFS ", true,
                CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(ParameterNaming.USER,
                "User that will perform the operation, if is not set the default web user is used", false,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.INPUT_FILE_PATH,
                "Input path for the file or ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.SEPARATOR,
                "Separator of the delimited file(s) ", true,
                CustomWrapperInputParameterTypeFactory.stringType()) };


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    /*
     * Defines a custom configuration in order not to delegate the projections.
     */
    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration configuration = new CustomWrapperConfiguration();
        configuration.setDelegateProjections(false);
        // Equals operator is delegated in searchable fields. Other operator
        // will be postprocessed
        configuration.setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ });
        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        return new CustomWrapperSchemaParameter[] {
            new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_KEY,
                java.sql.Types.VARCHAR, null, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, isNullable, !isMandatory),
            new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_VALUE,
                java.sql.Types.VARCHAR, null, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE,
                !isUpdateable, isNullable, !isMandatory) };
    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        String host = (String) getInputParameterValue(ParameterNaming.HOST_IP).getValue();
        int port = ((Integer) getInputParameterValue(ParameterNaming.HOST_PORT).getValue()).intValue();
        CustomWrapperInputParameterValue userParamValue =  getInputParameterValue(ParameterNaming.USER);
        String user = null;
        if (userParamValue != null && StringUtils.isNotBlank((String) userParamValue.getValue())) {
            user = (String) userParamValue.getValue();
        }
        String inputFilePath = (String) getInputParameterValue(ParameterNaming.INPUT_FILE_PATH).getValue();
        String separator = (String) getInputParameterValue(ParameterNaming.SEPARATOR).getValue();

        DefaultHttpClient httpClient = new DefaultHttpClient();
        BufferedReader br = null;
        try {
            HttpRequestBase httpRequest = new HttpGet();
            URI uri = buildOpenOperationURI(host, port, user, inputFilePath);
            httpRequest = new HttpGet(uri);
            if (logger.isDebugEnabled()) {
                logger.debug("URI: " + uri);
            }
            HttpResponse response = httpClient.execute(httpRequest);
            int statusCode = response.getStatusLine().getStatusCode();
            String statusMessage = response.getStatusLine().getReasonPhrase();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                InputStream is = responseEntity.getContent();
                br = new BufferedReader(new InputStreamReader(is));
                String line;
                String[] asArray = null;
                while ((line = br.readLine()) != null) {
                    asArray = line.split(separator);
                    if (asArray.length != 2) {
                        throw new InternalErrorException(String.format(
                            "Error reading line: line does not contain the specified separator '%s' ",
                            separator));
                    }
                    result.addRow(asArray, projectedFields);
                }
            } else {
                throw new InternalErrorException("Path '" + inputFilePath
                    + "': HTTP error code " + statusCode + ". " + statusMessage);
            }
        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            if (br != null) {
                IOUtils.closeQuietly(br);
            }
            httpClient.getConnectionManager().shutdown();
        }
    }

    /**
     *  URI for OPEN operation:
     *  http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN[&user.name=<USER>][&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]
     */
    private static URI buildOpenOperationURI(String host, int port, String username,
        String path) {

        StringBuilder uriSb = new StringBuilder("http://" + host + ":" + port
            + REST_API_PREFIX + URLUtil.encode(path) + "?op=OPEN");
        if (StringUtils.isNotBlank(username)) {
            uriSb.append("&user.name=" + username);
        }

        return URI.create(uriSb.toString());
    }

}
