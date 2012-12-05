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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hdfs.wrapper.util.ExceptionUtil;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;

/**
 * HDFS File Connector Custom Wrapper for reading key-value delimited text files
 * stored in HDFS (Hadoop Distributed File System)
 * <p>
 * 
 * You will be asked Namenode host, Namenode port, file path and file delimiter.
 * <br/>
 * If everything works fine, the key-value pairs contained in the file will be
 * returned by the wrapper
 * </p>
 * 
 * @see AbstractCustomWrapper
 */
public class HdfsSimpleFileWrapper extends AbstractCustomWrapper {
    private static Logger logger = Logger
            .getLogger(HdfsSimpleFileWrapper.class);

    private static final String INPUT_PARAMETER_NAMENODE_HOST = "Host";
    private static final String INPUT_PARAMETER_NAMENODE_PORT = "Port";
    private static final String INPUT_PARAMETER_DELETE_AFTER_READING = "Delete after reading";

    private static final String SCHEMA_FILE_COLUMN_DELIMITER = "column_delimiter";
    private static final String SCHEMA_PARAMETER_INPUT_FILE = "file_path";
    private static final String SCHEMA_KEY = "key";
    private static final String SCHEMA_VALUE = "value";

    /*
     * Hashmap that stored the position of the columns in the output for quick
     * access
     */
    private Map<String, Integer> cwColumnIndexes = new HashMap<String, Integer>();

    /*
     * Stores the wrapper's schema
     */
    private CustomWrapperSchemaParameter[] schema = null;

    @Override
    public CustomWrapperConfiguration getConfiguration() {
        return super.getConfiguration();
    }

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
                new CustomWrapperInputParameter(
                        INPUT_PARAMETER_NAMENODE_HOST,
                        "Namenode hostname or IP, e.g., localhost or 192.168.1.3 ",
                        true, CustomWrapperInputParameterTypeFactory
                                .stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_NAMENODE_PORT,
                        "Namenode port, e.g., 8020 ", true,
                        CustomWrapperInputParameterTypeFactory.integerType()),
                new CustomWrapperInputParameter(
                        INPUT_PARAMETER_DELETE_AFTER_READING,
                        "Delete file after reading it?", true,
                        CustomWrapperInputParameterTypeFactory
                                .booleanType(false)) };
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
                new CustomWrapperSchemaParameter(SCHEMA_PARAMETER_INPUT_FILE,
                        java.sql.Types.VARCHAR, null, isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_FILE_COLUMN_DELIMITER,
                        java.sql.Types.VARCHAR, null, isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_KEY,
                        java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(SCHEMA_VALUE,
                        java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE,
                        !isUpdeatable, isNullable, !isMandatory) };
        return this.schema;
    }

    public void run(CustomWrapperConditionHolder condition,
            List<CustomWrapperFieldExpression> projectedFields,
            CustomWrapperResult result, Map<String, String> inputValues)
            throws CustomWrapperException {
        // Due to getContextClassLoader returning the platform classloader, we
        // need to modify it in order to allow
        // Hadoop and Avro fetch certain classes -it uses getContextClassLoader
        ClassLoader originalCtxClassLoader = Thread.currentThread()
                .getContextClassLoader();
        Thread.currentThread().setContextClassLoader(
                Configuration.class.getClassLoader());

        if (logger.isDebugEnabled()) {
            logger.debug("Running custom wrapper: " + this.getClass());
            logger.debug("Input values: ");
            for (Entry<String, String> inputParam : inputValues.entrySet()) {
                logger.debug(String.format("%s : %s", inputParam.getKey(),
                        inputParam.getValue()));
            }
        }

        // Get projected fields positions
        getColumnPositions(projectedFields);
        if (logger.isDebugEnabled())
            logger.debug("cwColumnIndexes: " + this.cwColumnIndexes + "\n");

        // String host = "192.168.153.10" int port = 8020
        String host = (String) getInputParameterValue(
                INPUT_PARAMETER_NAMENODE_HOST).getValue();
        int port = (Integer) getInputParameterValue(
                INPUT_PARAMETER_NAMENODE_PORT).getValue();
        boolean delete_after_reading = (Boolean) getInputParameterValue(
                INPUT_PARAMETER_DELETE_AFTER_READING).getValue();

        // Establishing configuration
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition
                .getConditionMap();
        String input_file_path = "";
        String column_delimiter = "";

        if (conditionMap != null) {
            for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
                Object value = conditionMap.get(field);
                if (field.getName().equals(SCHEMA_PARAMETER_INPUT_FILE)) {
                    input_file_path = (String) value;
                }
                if (field.getName().equals(SCHEMA_FILE_COLUMN_DELIMITER)) {
                    column_delimiter = (String) value;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Accessing file: " + input_file_path + " at: "
                    + "hdfs://" + host + ":" + port);
            logger.debug("Using column delimiter: " + column_delimiter);
        }
        // File to read
        Path input_path = new Path(input_file_path);
        FileSystem fileSystem = null;
        FSDataInputStream dataInputStream = null;
        try {
            fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(input_path)) {
                FileStatus[] fss = fileSystem.listStatus(input_path);
                for (FileStatus status : fss) {
                    Path path = status.getPath();
                    if (!status.isDir()) {
                        dataInputStream = fileSystem.open(path);
                        String line = "";
                        String[] row = new String[this.cwColumnIndexes.size()];
                        while ((line = dataInputStream.readLine()) != null) {
                            String[] line_array = line.split(column_delimiter);
                            // column_delimiter matches the key/value delimiter
                            if (line_array.length == 2) {
                                if (this.cwColumnIndexes
                                        .containsKey(SCHEMA_KEY)) {
                                    row[this.cwColumnIndexes.get(SCHEMA_KEY)
                                            .intValue()] = line_array[0];
                                }
                                if (this.cwColumnIndexes
                                        .containsKey(SCHEMA_VALUE)) {
                                    row[this.cwColumnIndexes.get(SCHEMA_VALUE)
                                            .intValue()] = line_array[1];
                                }
                                if (this.cwColumnIndexes
                                        .containsKey(SCHEMA_PARAMETER_INPUT_FILE))
                                    row[this.cwColumnIndexes.get(
                                            SCHEMA_PARAMETER_INPUT_FILE)
                                            .intValue()] = input_file_path;
                                if (this.cwColumnIndexes
                                        .containsKey(SCHEMA_FILE_COLUMN_DELIMITER))
                                    row[this.cwColumnIndexes.get(
                                            SCHEMA_FILE_COLUMN_DELIMITER)
                                            .intValue()] = column_delimiter;
                                result.addRow(row, projectedFields);

                            } else {
                                logger.debug("Column delimiter does not match the key/value delimiter");
                                throw new CustomWrapperException(
                                        "Column delimiter does not match the key/value delimiter");
                            }
                        }
                    }
                }
                try {
                    if (delete_after_reading) {
                        // Delete path recursively after reading
                        fileSystem.delete(input_path, true);
                        if (logger.isDebugEnabled())
                            logger.debug("Deleted path " + input_file_path);
                    }
                } catch (IOException e) {
                    String stack = ExceptionUtil.getStacktraceAsString(e);
                    logger.error("IOException: " + stack);
                    throw new CustomWrapperException("IOException: " + stack, e);
                } finally {
                    if (dataInputStream != null)
                        dataInputStream.close();
                    if (fileSystem != null)
                        fileSystem.close();
                }
            } else {
                logger.error("Path not found " + input_file_path);
                if (fileSystem != null)
                    fileSystem.close();
                throw new CustomWrapperException("Path not found "
                        + input_file_path);
            }
        } catch (Exception e) {
            String stack = ExceptionUtil.getStacktraceAsString(e);
            logger.error("Exception while executing wrapper: " + stack);
            throw new CustomWrapperException(
                    "Exception while executing wrapper: " + stack, e);
        } finally {
            Thread.currentThread()
                    .setContextClassLoader(originalCtxClassLoader);
        }
    }

    /**
     * Gets the position of the projected fields
     * 
     * @param projectedFields
     *            the list of projected fields
     * @throws CustomWrapperException
     */

    private void getColumnPositions(
            List<CustomWrapperFieldExpression> projectedFields)
            throws CustomWrapperException {
        Integer index = 0;
        for (CustomWrapperFieldExpression col : projectedFields) {
            this.cwColumnIndexes.put(col.getName().toLowerCase(), index);
            index++;
        }
    }
}