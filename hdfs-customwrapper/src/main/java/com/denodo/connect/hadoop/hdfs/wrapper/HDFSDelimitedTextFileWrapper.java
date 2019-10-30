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
import java.io.InputStream;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.reader.HDFSDelimitedFileReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.util.csv.CSVConfig;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

/**
 * HDFS file custom wrapper for reading delimited text files stored in HDFS
 * (Hadoop Distributed File System).
 * <p>
 *
 * The following parameters are required: file system URI, file path
 * and file separator. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class HDFSDelimitedTextFileWrapper extends AbstractHDFSKeyValueFileWrapper {
    
    private static final  Logger LOG = LoggerFactory.getLogger(HDFSDelimitedTextFileWrapper.class); 
    

    private static final  CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
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
            new CustomWrapperInputParameter(Parameter.IGNORE_MATCHING_ERRORS, "Ignore the lines of this file that do not have the expected number of columns ",
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(true))
    };


    public HDFSDelimitedTextFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] doGetInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(super.doGetInputParameters(), INPUT_PARAMETERS);
    }
    
    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        final boolean isSearchable = true;
        final boolean isUpdateable = true;
        final boolean isNullable = true;
        final boolean isMandatory = true;
        boolean includePathColumn = false;
        final boolean header = Boolean.parseBoolean(inputValues.get(Parameter.HEADER));
        Object[] headerNames = readHeader(inputValues);
        if (!header) {
            headerNames = buildSyntheticHeader(headerNames.length);
        }
        int lengthSchemaParameters = headerNames.length;
        if(Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN))){
            includePathColumn = true;
            lengthSchemaParameters++;
        }
        final CustomWrapperSchemaParameter[] headerSchema =  new CustomWrapperSchemaParameter[lengthSchemaParameters];
        int i = 0;
        for (final Object item : headerNames) {
            if (item == null) {
                throw new CustomWrapperException("Header has null values and this is not allowed. Fix the file or try to clear the check box 'Header'.");
            }
            headerSchema[i++] = new CustomWrapperSchemaParameter(item.toString(), Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, isNullable, !isMandatory);

        }
        if(includePathColumn){
            headerSchema[i++] = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, isNullable, !isMandatory);
        }
        return headerSchema;
    }

    private Object[] readHeader(final Map<String, String> inputValues) throws CustomWrapperException {

        HDFSFileReader reader = null;
        try {

            final Map<String, String> values = tuneInput(inputValues);
            reader = getHDFSFileReader(values, true);
            final Object[] headerNames = (Object[]) reader.read();

            if (headerNames == null) {
                throw new CustomWrapperException("There are no files in " + inputValues.get(Parameter.FILE_PATH) 
                    + (StringUtils.isNotBlank(inputValues.get(Parameter.FILE_NAME_PATTERN)) 
                        ? " matching the provided file pattern: " + inputValues.get(Parameter.FILE_NAME_PATTERN)
                        : ""));
            }
            
            return headerNames;
            
        } catch (final Exception e) {
            LOG.error("Error accessing HDFS file", e);
            throw new CustomWrapperException("Error accessing HDFS file: " + e.getMessage(), e);
        } finally {

            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }

        }
    }

    /*
     * Tune configuration values for non skipping header, because we are interested in the header.
     */
    private Map<String, String> tuneInput(final Map<String, String> inputValues) {
        
        final Map<String, String> values = new HashMap<String, String>(inputValues);
        values.put(Parameter.HEADER, Boolean.toString(false));
        
        return values;
    }   
    
    private static String[] buildSyntheticHeader(final int size) {
        
        final String[] header = new String[size];
        for (int i = 0; i < size; i++) {
            header[i] = "column" + i;
        }
        
        return header;
    }
    
    private static CSVConfig getConfig(final Map<String, String> inputValues) {
        return new CSVConfig(inputValues.get(Parameter.SEPARATOR),
                inputValues.get(Parameter.QUOTE),
                inputValues.get(Parameter.COMMENT_MARKER),
                inputValues.get(Parameter.ESCAPE),
                Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_SPACES)),
                Boolean.parseBoolean(inputValues.get(Parameter.HEADER)),
                inputValues.get(Parameter.NULL_VALUE));
    }

    @Override
    public HDFSFileReader getHDFSFileReader(final Map<String, String> inputValues, boolean getSchemaParameters)
        throws IOException, InterruptedException, CustomWrapperException {
        
        final Configuration conf = getHadoopConfiguration(inputValues);
        final String inputFilePath = inputValues.get(Parameter.FILE_PATH);
        final Path path = new Path(inputFilePath);
        
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        return new HDFSDelimitedFileReader(conf, getConfig(inputValues), path, fileNamePattern, null, includePathColumn && !getSchemaParameters);
    }

    @Override
    public boolean ignoreMatchingErrors(final Map<String, String> inputValues) {
        return Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_MATCHING_ERRORS));
    }

}
