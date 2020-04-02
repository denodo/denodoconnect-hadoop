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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.util.csv.CSVConfig;
import com.denodo.connect.hadoop.hdfs.util.csv.CSVReader;
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
 * CSV records contained in the file will be returned by the wrapper.
 * 
 * @deprecated  Since 2018 versions.
 *    Use VDP standard data sources as XML, JSON or Delimited file or
 *    HDFS Custom Wrappers with webhdfs scheme.
 *    Both alternatives are more flexible and powerful.
 */
@Deprecated
public class WebHDFSFileWrapper extends AbstractCustomWrapper {

    private static final  Logger logger = LoggerFactory.getLogger(WebHDFSFileWrapper.class); 

    
    private boolean stopRequested = false;


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILE_PATH, "Absolute file path ",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.SEPARATOR, "Separator of the delimited file. Default is ',' ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.QUOTE, "Character used to encapsulate values containing special characters. Default is '\"' ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.COMMENT_MARKER, "Character marking the start of a line comment. Default is: comments not supported ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ESCAPE, "Escape character. Default is: escapes not supported  ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.NULL_VALUE, "String used to represent a null value. Default is: none, nulls are not distinguished from empty strings  ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.IGNORE_SPACES, "Spaces around values are ignored. ",
                true, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.HEADER, "The file has header ",
                true, true, CustomWrapperInputParameterTypeFactory.booleanType(true)),
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING, "Delete the file after reading it? ",
                true, true, CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.FILE_ENCODING, "The file encoding, system encoding is used by default.",
                false, true, CustomWrapperInputParameterTypeFactory.stringType())
        };
    }

    @Override
    public CustomWrapperInputParameter[] getDataSourceInputParameters() {
        return new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.HOST_IP, "Host IP or <bucket>.s3.amazonaws.com for Amazon S3 ",
                true, true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.HOST_PORT, "HTTP port: default port for WebHDFS is 50075. For HttpFS is 14000. ",
                true, true, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(Parameter.USER, "User that will perform the operation or <id>:<secret> for Amazon S3 ",
                false, true, CustomWrapperInputParameterTypeFactory.stringType())
        };
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        final boolean isSearchable = true;
        final boolean isUpdateable = true;
        final boolean isNullable = true;
        final boolean isMandatory = true;
        
        final boolean header = Boolean.parseBoolean(inputValues.get(Parameter.HEADER));
        List<String> headerNames = readHeader(inputValues);
        if (!header) {
            headerNames = buildSyntheticHeader(headerNames.size());
        }

        final CustomWrapperSchemaParameter[] headerSchema =  new CustomWrapperSchemaParameter[headerNames.size()];
        int i = 0;
        for (final String item : headerNames) {
            headerSchema[i++] = new CustomWrapperSchemaParameter(item, Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, isNullable, !isMandatory);

        }
        
        return headerSchema;
    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
        final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues)
        throws CustomWrapperException {


        final String host = inputValues.get(Parameter.HOST_IP);
        final int port = Integer.parseInt(inputValues.get(Parameter.HOST_PORT));
        final String user = inputValues.get(Parameter.USER);
        String filePath = StringUtils.trim(inputValues.get(Parameter.FILE_PATH));
        filePath = normalizePath(filePath);
        final CSVConfig csvConfig = getConfig(inputValues);
        final boolean header = Boolean.parseBoolean(inputValues.get(Parameter.HEADER));
        final boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        final DefaultHttpClient httpClient = new DefaultHttpClient();
        BufferedReader br = null;
        CSVReader csvReader = null;
        try {

            final URI openURI = URIUtils.getWebHDFSOpenURI(host, port, user, filePath);
            final InputStream is = HTTPUtils.requestGet(openURI, httpClient);
            if(csvConfig.isFileEncoding()){
                br = new BufferedReader(new InputStreamReader(is, csvConfig.getFileEncoding()));
            } else {
                br = new BufferedReader(new InputStreamReader(is));
            }
            csvReader = new CSVReader(br, csvConfig);
            if (header && csvReader.hasNext()) {
                csvReader.next(); // skip header
            }
            while (csvReader.hasNext() && !this.stopRequested) {
                
                final List<String> data = csvReader.next();
                result.addRow(data.toArray(), projectedFields);
            }

            if (delete) {
                try {
                    final URI deleteURI = URIUtils.getWebHDFSDeleteURI(host, port, user, filePath);
                    HTTPUtils.requestDelete(deleteURI, httpClient);
                } catch (final Exception e) {
                    logger.error("Error deleting the file", e);
                }
            }

        } catch (final Exception e) {
            logger.error("Error accessing WebHDFS", e);
            throw new CustomWrapperException("Error accessing WebHDFS: " + e.getMessage(), e);
        } finally {
            if (csvReader != null) {
                csvReader.close();
            }
            
            IOUtils.closeQuietly(br);
            httpClient.getConnectionManager().shutdown();

        }
    }

    private static List<String> readHeader(final Map<String, String> inputValues) throws CustomWrapperException {

        final String host = inputValues.get(Parameter.HOST_IP);
        final int port = Integer.parseInt(inputValues.get(Parameter.HOST_PORT));
        final String user = inputValues.get(Parameter.USER);
        String filePath = StringUtils.trim(inputValues.get(Parameter.FILE_PATH));
        filePath = normalizePath(filePath);
        final CSVConfig csvConfig = getConfig(inputValues);

        final DefaultHttpClient httpClient = new DefaultHttpClient();
        BufferedReader br = null;
        CSVReader csvReader = null;
        try {

            final URI openURI = URIUtils.getWebHDFSOpenURI(host, port, user, filePath);
            final InputStream is = HTTPUtils.requestGet(openURI, httpClient);
            if(csvConfig.isFileEncoding()){
                br = new BufferedReader(new InputStreamReader(is, csvConfig.getFileEncoding()));
            } else {
                br = new BufferedReader(new InputStreamReader(is));
            }
            csvReader = new CSVReader(br, csvConfig);

            if (csvReader.hasNext()) {
                return csvReader.next();
            }
            
            return Collections.emptyList();

        } catch (final Exception e) {
            logger.error("Error accessing WebHDFS", e);
            throw new CustomWrapperException("Error accessing WebHDFS: " + e.getMessage(), e);
        } finally {
            if (csvReader != null) {
                csvReader.close();
            }

            IOUtils.closeQuietly(br);
            httpClient.getConnectionManager().shutdown();

        }
    }
    
    private static CSVConfig getConfig(final Map<String, String> inputValues) {
        return new CSVConfig(inputValues.get(Parameter.SEPARATOR),
                inputValues.get(Parameter.QUOTE),
                inputValues.get(Parameter.COMMENT_MARKER),
                inputValues.get(Parameter.ESCAPE),
                Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_SPACES)),
                Boolean.parseBoolean(inputValues.get(Parameter.HEADER)),
                inputValues.get(Parameter.NULL_VALUE),
                inputValues.get(Parameter.FILE_ENCODING));
    }
    
    private static List<String> buildSyntheticHeader(final int size) {
        
        final List<String> header = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            header.add("column" + i);
        }
        
        return header;
    }
    
    @Override
    public boolean stop() {
        this.stopRequested = true;
        return true;
    }

    private static String normalizePath(final String filePath) {

        if (!filePath.startsWith("/")) {
            return '/' + filePath;
        }

        return filePath;
    }

}
