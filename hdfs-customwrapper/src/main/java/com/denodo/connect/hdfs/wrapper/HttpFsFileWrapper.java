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
package com.denodo.connect.hdfs.wrapper;

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

import com.denodo.connect.hdfs.wrapper.util.ExceptionUtil;
import com.denodo.util.exceptions.UnsupportedOperationException;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapper;
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
public class HttpFsFileWrapper extends AbstractCustomWrapper {
    private static final String URL_PREFIX = "/webhdfs/v1";
    /**
     * @see http://hadoop.apache.org/docs/r1.0.3/webhdfs.html#OPEN
     */
    private static final String OPEN = "OPEN";
    /**
     * @see http://hadoop.apache.org/docs/r1.0.3/webhdfs.html#DELETE
     */
    private static final String DELETE = "DELETE";
    /**
     * @see http://hadoop.apache.org/docs/r1.0.3/webhdfs.html#CREATE
     */
    private static final String CREATE = "CREATE";

    /**
     * Logger for this class
     */
    private static Logger logger = Logger.getLogger(HttpFsFileWrapper.class);

    private static final String INPUT_PARAMETER_HTTPFS_HOST = "Host";
    private static final String INPUT_PARAMETER_HTTPFS_PORT = "Port";
    private static final String INPUT_PARAMETER_HTTPFS_USERNAME = "Username";
    private static final String INPUT_PARAMETER_DELETE_AFTER_READING = "Delete after reading";

    private static final String SCHEMA_PARAMETER_PATH = "path";
    // private static final String SCHEMA_PARAMETER_OPERATION = "operation";
    private static final String SCHEMA_FILE_COLUMN_DELIMITER = "column_delimiter";
    private static final String SCHEMA_STATUSCODE = "status_code";
    private static final String SCHEMA_STATUSMESSAGE = "status_message";
    private static final String SCHEMA_KEY = "key";
    private static final String SCHEMA_VALUE = "value";
    private static final String SCHEMA_KEY_VALUE_PAIRS = "key_values";

    /*
     * Stores the wrapper's schema
     */
    private CustomWrapperSchemaParameter[] schema = null;

    /**
     * Defines the input parameters of the Custom Wrapper
     * <p>
     * CustomWrapperInputParameter(String name, String description, boolean
     * isMandatory, CustomWrapperInputParameterType type)
     * </p>
     */
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
                new CustomWrapperInputParameter(INPUT_PARAMETER_HTTPFS_HOST,
                        "HttpFS host, e.g., localhost or 192.168.1.3 ", true,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_HTTPFS_PORT,
                        "HttpFS port, e.g., 14000 ", true,
                        CustomWrapperInputParameterTypeFactory.integerType()),
                new CustomWrapperInputParameter(
                        INPUT_PARAMETER_HTTPFS_USERNAME,
                        "User that will perform the operation, e.g., cloudera",
                        true,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(
                        INPUT_PARAMETER_DELETE_AFTER_READING,
                        "Delete file after reading it?", true,
                        CustomWrapperInputParameterTypeFactory
                                .booleanType(false)) };
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
        configuration
                .setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ });
        return configuration;
    }

    /**
     * @see CustomWrapper#getSchemaParameters()
     */
    public CustomWrapperSchemaParameter[] getSchemaParameters(
            Map<String, String> inputValues) throws CustomWrapperException {
        boolean isSearchable = true;
        boolean isUpdeatable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        if (this.schema != null) {
            return this.schema;
        }
        this.schema = new CustomWrapperSchemaParameter[] {
                new CustomWrapperSchemaParameter(SCHEMA_PARAMETER_PATH,
                        java.sql.Types.VARCHAR, null, isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, isMandatory),
                // new CustomWrapperSchemaParameter(SCHEMA_PARAMETER_OPERATION,
                // java.sql.Types.VARCHAR, null, !isSearchable,
                // CustomWrapperSchemaParameter.NOT_SORTABLE,
                // !isUpdeatable, isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_FILE_COLUMN_DELIMITER,
                        java.sql.Types.VARCHAR, null, isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_STATUSCODE,
                        java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_STATUSMESSAGE,
                        java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(
                        SCHEMA_KEY_VALUE_PAIRS,
                        java.sql.Types.ARRAY,
                        new CustomWrapperSchemaParameter[] {
                                new CustomWrapperSchemaParameter(
                                        SCHEMA_KEY,
                                        java.sql.Types.VARCHAR,
                                        null,
                                        !isSearchable,
                                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                                        !isUpdeatable, isNullable, !isMandatory),
                                new CustomWrapperSchemaParameter(
                                        SCHEMA_VALUE,
                                        java.sql.Types.VARCHAR,
                                        null,
                                        !isSearchable,
                                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                                        !isUpdeatable, isNullable, !isMandatory) }) };
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
                logger.debug(String.format("%s : %s", inputParam.getKey(),
                        inputParam.getValue()));
            }
        }
        String host = (String) getInputParameterValue(
                INPUT_PARAMETER_HTTPFS_HOST).getValue();
        int port = (Integer) getInputParameterValue(INPUT_PARAMETER_HTTPFS_PORT)
                .getValue();
        String username = (String) getInputParameterValue(
                INPUT_PARAMETER_HTTPFS_USERNAME).getValue();
        boolean delete_after_reading = (Boolean) getInputParameterValue(
                INPUT_PARAMETER_DELETE_AFTER_READING).getValue();

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition
                .getConditionMap();
        // String operation = "";
        String operation = OPEN;
        String path = "";
        String column_delimiter = "";
        if (conditionMap != null) {
            for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
                Object value = conditionMap.get(field);
                // if (field.getName().equals(SCHEMA_PARAMETER_OPERATION)) {
                // operation = (String) value;
                // }
                if (field.getName().equals(SCHEMA_PARAMETER_PATH)) {
                    path = (String) value;
                }
                if (field.getName().equals(SCHEMA_FILE_COLUMN_DELIMITER)) {
                    column_delimiter = (String) value;
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
            if (logger.isDebugEnabled())
                logger.debug("URI: " + uri);
            try {
                HttpResponse response = httpClient.execute(httpRequest);
                int status_code = response.getStatusLine().getStatusCode();
                String status_message = response.getStatusLine()
                        .getReasonPhrase();
                List<String[]> key_value_pairs = new ArrayList<String[]>();
                if (status_code == HttpStatus.SC_OK) {
                    HttpEntity responseEntity = response.getEntity();
                    InputStream is = responseEntity.getContent();
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(is));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] line_array = line.split(column_delimiter);
                        if (line_array.length == 2) {
                            key_value_pairs.add(new String[] { line_array[0],
                                    line_array[1] });
                        } else {
                            logger.debug("Column delimiter does not match the key/value delimiter");
                            throw new CustomWrapperException(
                                    "Column delimiter does not match the key/value delimiter");
                        }
                    }
                }
                result.addRow(
                        new Object[] { path,/* operation, */
                        column_delimiter, status_code, status_message,
                                key_value_pairs.toArray() }, projectedFields);
                try {
                    if (delete_after_reading) {
                        // Delete path recursively after reading
                        // "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]"
                        httpRequest.setURI(URI.create("http://" + host + ":"
                                + port + URL_PREFIX + path + "?user.name="
                                + username + "&op=" + DELETE
                                + "&recursive=true"));
                        response = httpClient.execute(httpRequest);
                        status_code = response.getStatusLine().getStatusCode();
                        if (status_code == HttpStatus.SC_OK) {
                            if (logger.isDebugEnabled())
                                logger.debug("Deleted path " + path);
                            return;
                        } else {
                            logger.error("HttpError while deleting file: '"
                                    + path
                                    + "' with status code: '"
                                    + response.getStatusLine().getStatusCode()
                                    + "' | status message: '"
                                    + response.getStatusLine()
                                            .getReasonPhrase() + "'");
                            throw new CustomWrapperException(
                                    "HttpError while deleting file: '"
                                            + path
                                            + "' with status code: '"
                                            + response.getStatusLine()
                                                    .getStatusCode()
                                            + "' | status message: '"
                                            + response.getStatusLine()
                                                    .getReasonPhrase() + "'");
                        }
                    }
                } catch (Exception e) {
                    String stack = ExceptionUtil.getStacktraceAsString(e);
                    logger.error("Exception when trying to delete file: '"
                            + path + "' " + stack);
                    throw new CustomWrapperException(
                            "Exception when trying to delete file: '" + path
                                    + "' " + stack, e);
                }
            } catch (Exception e) {
                String stack = ExceptionUtil.getStacktraceAsString(e);
                logger.error("Error occurred while running custom wrapper:  "
                        + stack);
                throw new CustomWrapperException(
                        "Error occurred while running custom wrapper: " + stack,
                        e);
            }
        } catch (IllegalArgumentException e) {
            String stack = ExceptionUtil.getStacktraceAsString(e);
            logger.error("The given string violates RFC 2396: '" + uri + "' "
                    + stack);
            throw new CustomWrapperException(
                    "The given string violates RFC 2396: '" + uri + "' "
                            + stack, e);
        } catch (Exception e) {
            String stack = ExceptionUtil.getStacktraceAsString(e);
            logger.error("Error occurred while running custom wrapper: "
                    + stack);
            throw new CustomWrapperException(
                    "Error occurred while running custom wrapper:  " + stack, e);
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
    private static HttpRequestBase getHttpOperation(String operation)
            throws UnsupportedOperationException {
        if (operation.equalsIgnoreCase(OPEN))
            return new HttpGet();
        // else if (operation.equalsIgnoreCase(DELETE))
        // return new HttpDelete();
        // else if (operation.equalsIgnoreCase(CREATE))
        // return new HttpPost();
        else
            throw new UnsupportedOperationException("Operation '" + operation
                    + "' is not supported");
    }
}
