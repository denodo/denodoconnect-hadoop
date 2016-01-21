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
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.reader.HDFSDelimitedFileReader;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
import com.denodo.connect.hadoop.hdfs.util.csv.CSVConfig;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

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
    
    private static final Logger logger = Logger.getLogger(HDFSDelimitedTextFileWrapper.class);

    private static final  CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.SEPARATOR, "Separator of the delimited file. Default is ',' ",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.QUOTE, "Character used to encapsulate values containing special characters. Default is '\"' ", false,
                CustomWrapperInputParameterTypeFactory.stringType()),                
            new CustomWrapperInputParameter(Parameter.COMMENT_MARKER, "Character marking the start of a line comment. Default is: comments not supported ", false,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.ESCAPE, "Escape character. Default is: escapes not supported  ", false,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.IGNORE_SPACES, "Spaces around values are ignored. ", true,
                CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.HEADER, "The file has header ", true,
                CustomWrapperInputParameterTypeFactory.booleanType(true))                            
    };


    public HDFSDelimitedTextFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] doGetInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(super.doGetInputParameters(), INPUT_PARAMETERS);
    }
    
    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;
        
        boolean header = Boolean.parseBoolean(inputValues.get(Parameter.HEADER));
        Object[] headerNames = readHeader(inputValues);
        if (!header) {
            headerNames = buildSyntheticHeader(headerNames.length);
        }

        CustomWrapperSchemaParameter[] headerSchema =  new CustomWrapperSchemaParameter[headerNames.length];
        int i = 0;
        for (Object item : headerNames) {
            headerSchema[i++] = new CustomWrapperSchemaParameter(item.toString(), Types.VARCHAR, null, !isSearchable,
                    CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, isNullable, !isMandatory);

        }
        
        return headerSchema;
    }

    private Object[] readHeader(Map<String, String> inputValues) throws CustomWrapperException {

        HDFSFileReader reader = null;
        try {

            Map<String, String> values = tuneInput(inputValues);
            reader = getHDFSFileReader(values);
            return (Object[]) reader.read();

        } catch (Exception e) {
            logger.error("Error accessing HDFS file", e);
            throw new CustomWrapperException("Error accessing HDFS file: " + e.getMessage(), e);
        } finally {

            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error releasing the reader", e);
            }

        }
    }

    /*
     * Tune configuration values for non skipping header, because we are interested in the header.
     */
    private Map<String, String> tuneInput(Map<String, String> inputValues) {
        
        Map<String, String> values = new HashMap<String, String>(inputValues);
        values.put(Parameter.HEADER, Boolean.toString(false));
        
        return values;
    }   
    
    private static String[] buildSyntheticHeader(int size) {
        
        String[] header = new String[size];
        for (int i = 0; i < size; i++) {
            header[i] = "column" + i;
        }
        
        return header;
    }
    
    private static CSVConfig getConfig(Map<String, String> inputValues) {
        return new CSVConfig(inputValues.get(Parameter.SEPARATOR),
                inputValues.get(Parameter.QUOTE),
                inputValues.get(Parameter.COMMENT_MARKER),
                inputValues.get(Parameter.ESCAPE),
                Boolean.parseBoolean(inputValues.get(Parameter.IGNORE_SPACES)),
                Boolean.parseBoolean(inputValues.get(Parameter.HEADER)));
    }

    @Override
    public HDFSFileReader getHDFSFileReader(Map<String, String> inputValues) throws IOException, InterruptedException {

        String fileSystemURI = inputValues.get(Parameter.FILESYSTEM_URI);
        Configuration conf = HadoopConfigurationUtils.getConfiguration(fileSystemURI);
        String inputFilePath = inputValues.get(Parameter.FILE_PATH);
        Path path = new Path(inputFilePath);

        return new HDFSDelimitedFileReader(conf, getConfig(inputValues), path, null);
    }
}
