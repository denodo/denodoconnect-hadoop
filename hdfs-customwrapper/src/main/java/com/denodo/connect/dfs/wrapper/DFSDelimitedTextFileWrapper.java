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
package com.denodo.connect.dfs.wrapper;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.dfs.commons.naming.Parameter;
import com.denodo.connect.dfs.reader.DFSDelimitedFileReader;
import com.denodo.connect.dfs.reader.DFSFileReader;
import com.denodo.connect.dfs.util.csv.CSVConfig;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * DFS file custom wrapper for reading delimited text files stored in a DFS (Distributed File System: hdfs, S3, ...).
 * <p>
 *
 * The following parameters are required: file system URI, file path
 * and file separator. <br/>
 *
 * Key/value pairs contained in the file will be returned by the wrapper.
 * </p>
 *
 */
public class DFSDelimitedTextFileWrapper extends AbstractDFSKeyValueFileWrapper {
    
    private static final  Logger LOG = LoggerFactory.getLogger(DFSDelimitedTextFileWrapper.class);
    

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
                false, true, CustomWrapperInputParameterTypeFactory.booleanType(true)),
            new CustomWrapperInputParameter(Parameter.FILE_ENCODING, "The file encoding, system encoding is used by default.",
                false, true, CustomWrapperInputParameterTypeFactory.stringType())
    };


    public DFSDelimitedTextFileWrapper() {
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

        final String separator = inputValues.get(Parameter.SEPARATOR);
        final String quote = inputValues.get(Parameter.QUOTE);
        final String commentMarker = inputValues.get(Parameter.COMMENT_MARKER);
        final String escape = inputValues.get(Parameter.ESCAPE);
        final String nullValue = inputValues.get(Parameter.NULL_VALUE);
        final boolean ignoreSpaces = Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_SPACES));
        final boolean header = Boolean.parseBoolean(inputValues.get(Parameter.HEADER));

        if ((StringUtils.isNotBlank(separator)) && separator.length() > 1
            && !isInvisibleChars(separator) && (StringUtils.isNotBlank(quote) || StringUtils.isNotBlank(commentMarker)
            || StringUtils.isNotBlank(escape) || StringUtils.isNotBlank(nullValue) || ignoreSpaces == true)) {
            throw new CustomWrapperException("When a separator larger than one character is broken compatibility " +
                "with the standard comma-separated-value cannot be kept and therefore parameters Quote, Comment Marker, " +
                "Escape, Null value and Ignore Spaces are not supported");
        }

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

    private boolean isInvisibleChars(final String sep) {
        switch (sep) {
            case "\\t":
                return true;
            case "\\n":
                return true;
            case "\\r":
                return true;
            case "\\f":
                return true;

            default:
                return false;
        }
    }

    private Object[] readHeader(final Map<String, String> inputValues) throws CustomWrapperException {

        DFSFileReader reader = null;
        try {

            final Map<String, String> values = tuneInput(inputValues);
            reader = getDFSFileReader(values, true);
            final Object[] headerNames = (Object[]) reader.read();

            if (headerNames == null) {
                throw new CustomWrapperException("There are no files in " + inputValues.get(Parameter.FILE_PATH) 
                    + (StringUtils.isNotBlank(inputValues.get(Parameter.FILE_NAME_PATTERN)) 
                        ? " matching the provided file pattern: " + inputValues.get(Parameter.FILE_NAME_PATTERN)
                        : ""));
            }
            
            return headerNames;
            
        } catch (final Exception e) {
            LOG.error("Error accessing DFS file", e);
            throw new CustomWrapperException("Error accessing DFS file: " + e.getMessage(), e);
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
        
        final Map<String, String> values = new HashMap<>(inputValues);
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
                inputValues.get(Parameter.NULL_VALUE),
                inputValues.get(Parameter.FILE_ENCODING));
    }

    @Override
    public DFSFileReader getDFSFileReader(final Map<String, String> inputValues, final boolean getSchemaParameters)
        throws IOException, InterruptedException, CustomWrapperException {
        
        final Configuration conf = getConfiguration(inputValues);
        final String inputFilePath = StringUtils.trim(inputValues.get(Parameter.FILE_PATH));
        final Path path = new Path(inputFilePath);
        
        final String fileNamePattern = inputValues.get(Parameter.FILE_NAME_PATTERN);
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));

        return new DFSDelimitedFileReader(conf, getConfig(inputValues), path, fileNamePattern, null, includePathColumn && !getSchemaParameters);
    }

    @Override
    public boolean ignoreMatchingErrors(final Map<String, String> inputValues) {
        return Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_MATCHING_ERRORS));
    }

}
