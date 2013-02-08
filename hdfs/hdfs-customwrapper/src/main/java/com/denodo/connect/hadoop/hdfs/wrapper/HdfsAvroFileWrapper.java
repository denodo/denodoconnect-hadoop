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
package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.wrapper.util.AvroSchemaUtil;
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
 * HDFS File Connector Custom Wrapper for reading Avro files stored in HDFS
 * (Hadoop Distributed File System)
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
public class HdfsAvroFileWrapper extends AbstractCustomWrapper {
    private static Logger logger = Logger.getLogger(HdfsAvroFileWrapper.class);

    /**
     * The name of the 'Namenode host' input parameter for this wrapper
     */
    private static final String INPUT_PARAMETER_NAMENODE_HOST = "Host";
    /**
     * The name of the 'Namenode port' input parameter for this wrapper
     */
    private static final String INPUT_PARAMETER_NAMENODE_PORT = "Port";
    private static final String INPUT_PARAMETER_AVSC_FILE_PATH = "Avsc file path";
    private static final String INPUT_PARAMETER_USE_AVSC_FILE = "Use Avsc file";
    private static final String INPUT_PARAMETER_AVSC_SCHEMA = "Avsc schema";
    private static final String INPUT_PARAMETER_DELETE_AFTER_READING = "Delete after reading";

    private static final String SCHEMA_PARAMETER_AVRO_FILE_PATH = "avroFilepath";

    /*
     * Stores the wrapper's schema
     */
    private CustomWrapperSchemaParameter[] schema = null;
    private Schema avroSchema = null;

    public HdfsAvroFileWrapper() {
        super();
        // Due to getContextClassLoader returning the platform classloader, we
        // need to modify it in order to allow
        // Hadoop and Avro fetch certain classes -it uses getContextClassLoader
        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
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
                new CustomWrapperInputParameter(INPUT_PARAMETER_NAMENODE_HOST, "Namenode hostname or IP, e.g., localhost or 192.168.1.3 ",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_NAMENODE_PORT, "Namenode port, e.g., 8020 ", true,
                        CustomWrapperInputParameterTypeFactory.integerType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_FILE_PATH, "Path to the Avsc file", false,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_USE_AVSC_FILE, "Use the Avsc file for creating the schema?", true,
                        CustomWrapperInputParameterTypeFactory.booleanType(true)),
                new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_SCHEMA, "JSON definition of the Avro schema", false,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_DELETE_AFTER_READING, "Delete file after reading it?", true,
                        CustomWrapperInputParameterTypeFactory.booleanType(true)) };
    }

    /**
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

    /**
     * 
     * A schema may be one of:
     * 
     * <ul>
     * <li>A record, mapping field names to field value data</li>
     * <li>An enum, containing one of a small set of symbols</li>
     * <li>An array of values, all of the same schema</li>
     * <li>A map, containing string/value pairs, of a declared schema</li>
     * <li>A union of other schemas</li>
     * <li>A fixed sized binary object</li>
     * <li>A unicode string</li>
     * <li>A sequence of bytes</li>
     * <li>A 32-bit signed int</li>
     * <li>A 64-bit signed long</li>
     * <li>A 32-bit IEEE single-float</li>
     * <li>or A 64-bit IEEE double-float</li>
     * <li>or A boolean</li>
     * <li>or null</li>
     * </ul>
     * 
     * <p>
     * See the Avro documentation for more information (<a href="
     * http://avro.apache
     * .org/docs/1.5.4/api/java/org/apache/avro/Schema.html">here</a>)
     * </p>
     * 
     * @see CustomWrapper#getSchemaParameters()
     */
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues) throws CustomWrapperException {
        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        if (this.schema != null) {
            return this.schema;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Generating schema for custom wrapper: " + this.getClass());
            logger.debug("Input parameters: ");
            for (Entry<String, String> inputParam : inputValues.entrySet()) {
                logger.debug(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
            }
        }
        boolean useAvscFile = (Boolean) getInputParameterValue(INPUT_PARAMETER_USE_AVSC_FILE).getValue();
        if (useAvscFile) {
            String host = (String) getInputParameterValue(INPUT_PARAMETER_NAMENODE_HOST).getValue();
            int port = (Integer) getInputParameterValue(INPUT_PARAMETER_NAMENODE_PORT).getValue();
            String avscFilePath = (String) getInputParameterValue(INPUT_PARAMETER_AVSC_FILE_PATH).getValue();

            if (logger.isDebugEnabled()) {
                logger.debug("Accesing Avro Schema at: " + "hdfs://" + host + ":" + port + "\n File path: " + avscFilePath);
            }
            Configuration conf = new Configuration();
            // TO AVOID WEIRD "No FileSystem for scheme: hdfs" EXCEPTION
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.default.name", "hdfs://" + host + ":" + port);

            Path avscPath = new Path(avscFilePath);
            FileSystem fileSystem;
            FSDataInputStream dataInputStream = null;
            try {
                fileSystem = FileSystem.get(conf);
                dataInputStream = fileSystem.open(avscPath);
                this.avroSchema = Schema.parse(dataInputStream);
            } catch (IOException e) {
                String stackTrace = getStacktraceAsString(e);
                logger.error("Error getting Avro Schema: " + stackTrace, e);
                throw new CustomWrapperException("Error getting Avro Schema: " + stackTrace, e);
            } catch (Exception ee) {
                String stackTrace = getStacktraceAsString(ee);
                logger.error("Error generating base view schema: " + stackTrace, ee);
                throw new CustomWrapperException("Error generating base view schema: " + stackTrace, ee);
            }
        } else {
            String avscSchema = (String) getInputParameterValue(INPUT_PARAMETER_AVSC_SCHEMA).getValue();
            if (avscSchema != null)
                this.avroSchema = Schema.parse(avscSchema);
        }
        if (this.avroSchema != null) {

            CustomWrapperSchemaParameter avroSchemaParameter = AvroSchemaUtil.createSchemaParameter(this.avroSchema,
                    this.avroSchema.getName());
            this.schema = new CustomWrapperSchemaParameter[] {
                    new CustomWrapperSchemaParameter(SCHEMA_PARAMETER_AVRO_FILE_PATH, java.sql.Types.VARCHAR, null, !isSearchable,
                            CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable, !isNullable, isMandatory), avroSchemaParameter };
            return this.schema;
        } else {
            logger.error("Error generating base view. Avro schema is null");
            throw new CustomWrapperException("Error generating base view. Avro schema is null");
        }
    }

    public void run(CustomWrapperConditionHolder condition, List<CustomWrapperFieldExpression> projectedFields, CustomWrapperResult result,
            Map<String, String> inputValues) throws CustomWrapperException {
        // FIXME this.schema is null inside this method .... !
        this.schema = getSchemaParameters(inputValues);

        String host = (String) getInputParameterValue(INPUT_PARAMETER_NAMENODE_HOST).getValue();
        int port = (Integer) getInputParameterValue(INPUT_PARAMETER_NAMENODE_PORT).getValue();

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);
        // TO AVOID WEIRD "No FileSystem for scheme: hdfs" EXCEPTION
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        String avroFilePath = "";
        if (conditionMap != null) {
            for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
                Object value = conditionMap.get(field);
                if (field.getName().equals(SCHEMA_PARAMETER_AVRO_FILE_PATH)) {
                    avroFilePath = (String) value;
                }
            }
        } else

        if (logger.isDebugEnabled()) {
            logger.debug("Reading Avro file: " + avroFilePath + " in " + " hdfs://" + host + ":" + port);
            logger.debug("Using Avro schema: " + this.avroSchema);
        }
        // File to read
        Path path = new Path(avroFilePath);
        try {
            FsInput inputFile = new FsInput(path, conf);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(this.avroSchema);
            DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<GenericData.Record>(inputFile, reader);
            // Build the output object
            // FIXME Only GenericData.Record supported. Try GenericData.Array
            // FIXME Try GenericData.Fixed and GenericData.EnumSymbol
            if (this.schema != null) {
                for (GenericData.Record avroRecord : dataFileReader) {
                    Object[] rowData = new Object[this.schema.length];
                    rowData[0] = avroFilePath;
                    int i = 1;
                    // FIXME This only goes one level deep in the schema, it has
                    // to go to the deepest level
                    // FIXME Until no schema parameter with columns is found
                    for (CustomWrapperSchemaParameter schemaParam : this.schema) {
                        if (!schemaParam.getName().equalsIgnoreCase(SCHEMA_PARAMETER_AVRO_FILE_PATH)) {
                            if (schemaParam.getColumns().length > 0) {
                                Object[] cols = new Object[schemaParam.getColumns().length];
                                int j = 0;
                                for (CustomWrapperSchemaParameter schemaCols : schemaParam.getColumns()) {
                                    cols[j++] = ((GenericData.Record) avroRecord).get(schemaCols.getName());
                                }
                                rowData[i++] = cols;
                            } else
                                rowData[i++] = ((GenericData.Record) avroRecord).get(schemaParam.getName());
                        }
                    }
                    result.addRow(rowData, projectedFields);
                }
            } else {
                logger.error("This should never happen. Schema is null");
                throw new CustomWrapperException("his should never happen. Schema is null");
            }
        } catch (IOException ie) {
            String stackTrace = getStacktraceAsString(ie);
            logger.error("Error accesing Avro file: " + stackTrace);
            throw new CustomWrapperException("Error accesing Avro file: " + stackTrace, ie);

        } catch (Exception e) {
            String stackTrace = getStacktraceAsString(e);
            logger.error("Error runing custom wrapper: " + stackTrace);
            throw new CustomWrapperException("Error runing custom wrapper: " + stackTrace, e);
        }
    }

    private static String getStacktraceAsString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        return stackTrace;
    }
}
