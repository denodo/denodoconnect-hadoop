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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.denodo.util.exceptions.UnsupportedOperationException;
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

/**
 * HDFS File Connector Custom Wrapper for reading key-value delimited text files
 * stored in HDFS (Hadoop Distributed File System) using the HttpFs Gateway and
 * REST API
 * <ul>
 * Supported operations:
 * <li>OPEN</li>
 * </ul>
 * If everything works fine, the key-value pairs contained in the file will be
 * returned by the wrapper
 *
 * @see AbstractCustomWrapper
 */
// FIXME THIS IS A PROTOTYPE!!!!!
public class HttpFsFileWrapper extends AbstractCustomWrapper {

    /**
     * Logger for this class
     */
    private static Logger logger = Logger.getLogger(HttpFsFileWrapper.class);


    private static final String URL_PREFIX = "/webhdfs/v1";
    /**
     * @see http://hadoop.apache.org/docs/r1.0.3/webhdfs.html#OPEN
     */
    private static final String OPEN = "OPEN";

    private static final String INPUT_PARAMETER_HTTPFS_HOST = "Host";
    private static final String INPUT_PARAMETER_HTTPFS_PORT = "Port";
    private static final String INPUT_PARAMETER_HTTPFS_USERNAME = "Username";

    private static final String SCHEMA_PARAMETER_PATH = "path";
    private static final String SCHEMA_FILE_COLUMN_DELIMITER = "columnDelimiter";
    private static final String SCHEMA_STATUSCODE = "statusCode";
    private static final String SCHEMA_STATUSMESSAGE = "statusMessage";
    private static final String SCHEMA_KEY = "key";
    private static final String SCHEMA_VALUE = "value";
    private static final String SCHEMA_KEY_VALUE_PAIRS = "keyValues";


    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(INPUT_PARAMETER_HTTPFS_HOST,
                "HttpFS host, e.g., localhost or 192.168.1.3 ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_HTTPFS_PORT,
                "HttpFS port, e.g., 14000 ", true,
                CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_HTTPFS_USERNAME,
                "User that will perform the operation, e.g., cloudera", true,
                CustomWrapperInputParameterTypeFactory.stringType()) };

    private CustomWrapperSchemaParameter[] schema = null;


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
        configuration.setDelegateNotConditions(false);
        configuration.setDelegateOrConditions(false);
        configuration.setDelegateOrderBy(false);
        // Equals operator is delegated in searchable fields. Other operator
        // will be postprocessed
        configuration.setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ });
        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = false;
        boolean isNullable = true;
        boolean isMandatory = true;

        if (this.schema != null) {
            return this.schema;
        }
        this.schema =
            new CustomWrapperSchemaParameter[] {
                new CustomWrapperSchemaParameter(SCHEMA_PARAMETER_PATH,
                    java.sql.Types.VARCHAR, null, isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdateable,
                    isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_FILE_COLUMN_DELIMITER,
                    java.sql.Types.VARCHAR, null, isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdateable,
                    isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_STATUSCODE,
                    java.sql.Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdateable,
                    isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_STATUSMESSAGE,
                    java.sql.Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, isUpdateable,
                    isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_KEY_VALUE_PAIRS,
                    java.sql.Types.ARRAY, new CustomWrapperSchemaParameter[] {
                        new CustomWrapperSchemaParameter(SCHEMA_KEY,
                            java.sql.Types.VARCHAR, null, !isSearchable,
                            CustomWrapperSchemaParameter.NOT_SORTABLE,
                            isUpdateable, isNullable, !isMandatory),
                        new CustomWrapperSchemaParameter(SCHEMA_VALUE,
                            java.sql.Types.VARCHAR, null, !isSearchable,
                            CustomWrapperSchemaParameter.NOT_SORTABLE,
                            isUpdateable, isNullable, !isMandatory) }) };
        return this.schema;
    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        if (logger.isDebugEnabled()) {
            logger.debug("Running custom wrapper: " + this.getClass());
            logger.debug("Input values: ");
            for (Entry<String, String> inputParam : inputValues.entrySet()) {
                logger.debug(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
            }
        }
        String host = (String) getInputParameterValue(INPUT_PARAMETER_HTTPFS_HOST).getValue();
        int port = ((Integer) getInputParameterValue(INPUT_PARAMETER_HTTPFS_PORT).getValue()).intValue();
        String username = (String) getInputParameterValue(INPUT_PARAMETER_HTTPFS_USERNAME).getValue();

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        String operation = OPEN;
        String path = "";
        String columnDelimiter = "";
        if (conditionMap != null) {
            for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
                Object value = conditionMap.get(field);
                if (field.getName().equals(SCHEMA_PARAMETER_PATH)) {
                    path = (String) value;
                }
                if (field.getName().equals(SCHEMA_FILE_COLUMN_DELIMITER)) {
                    columnDelimiter = (String) value;
                }
            }
        }
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpRequestBase httpRequest = null;
        URI uri = null;
        try {
            httpRequest = getHttpOperation(operation);
            // Set request uri http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?op=..
            // FIXME HttpFS OPEN operation expects len, it should be length,
            // http://goo.gl/ANfKG
            // TODO use offset and len for large files "&offset=" + offset +
            // "&len=" + len
            uri = URI.create("http://" + host + ":" + port + URL_PREFIX + path
                + "?user.name=" + username + "&op=" + operation);
            httpRequest.setURI(uri);
            if (logger.isDebugEnabled()) {
                logger.debug("URI: " + uri);
            }
            try {
                HttpResponse response = httpClient.execute(httpRequest);
                int statusCode = response.getStatusLine().getStatusCode();
                String statusMessage = response.getStatusLine().getReasonPhrase();
                List<String[]> keyValuePairs = new ArrayList<String[]>();
                if (statusCode == HttpStatus.SC_OK) {
                    HttpEntity responseEntity = response.getEntity();
                    InputStream is = responseEntity.getContent();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] lineArray = line.split(columnDelimiter);
                        if (lineArray.length == 2) {
                            keyValuePairs.add(new String[] { lineArray[0], lineArray[1] });
                        } else {
                            logger.debug("Column delimiter does not match the key/value delimiter");
                            throw new CustomWrapperException("Column delimiter does not match the key/value delimiter");
                        }
                    }
                }
                result.addRow(new Object[] { path,/* operation, */
                columnDelimiter, Integer.valueOf(statusCode), statusMessage, keyValuePairs.toArray() }, projectedFields);

            } catch (Exception e) {
                logger.error("Error occurred while running custom wrapper:  ");
                throw new CustomWrapperException("Error occurred while running custom wrapper: ", e);
            }
        } catch (IllegalArgumentException e) {
            logger.error("The given string violates RFC 2396: '" + uri + "' ");
            throw new CustomWrapperException("The given string violates RFC 2396: '" + uri + "' ", e);
        } catch (Exception e) {
            logger.error("Error occurred while running custom wrapper: ");
            throw new CustomWrapperException("Error occurred while running custom wrapper: ", e);
        }
    }

    /**
     * Returns the HTTP method that corresponds to the given operation name
     *
     * @param operation
     *            name of the action to be performed against HDFS
     * @return the corresponding HTTP request method
     * @throws UnsupportedOperationException
     *             if the operation is not supported
     */
    // TODO Support DELETE and CREATE
    private static HttpRequestBase getHttpOperation(String operation) throws UnsupportedOperationException {

        if (operation.equalsIgnoreCase(OPEN)) {
            return new HttpGet();
        }
        throw new UnsupportedOperationException("Operation '" + operation + "' is not supported");
    }
}