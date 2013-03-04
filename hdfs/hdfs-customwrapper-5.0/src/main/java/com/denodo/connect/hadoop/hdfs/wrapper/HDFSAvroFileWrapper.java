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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.wrapper.commons.exception.InternalErrorException;
import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.hdfs.wrapper.reader.HDFSAvroFileReader;
import com.denodo.connect.hadoop.hdfs.wrapper.util.avro.AvroSchemaUtil;
import com.denodo.connect.hadoop.hdfs.wrapper.util.configuration.HadoopConfigurationUtils;
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
 * HDFS File Connector Custom Wrapper for reading Avro files stored in HDFS
 * (Hadoop Distributed File System)
 * <p>
 *
 * You will be asked for Namenode host, Namenode port, avro schema file path
 * or avro schema string.
 * <br/>
 * If everything works fine, the key-value pairs contained in the file will be
 * returned by the wrapper
 * </p>
 *
 */
public class HDFSAvroFileWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(HDFSAvroFileWrapper.class);


    /**
     * The path to the .avsc file containing the Avro schema.
     * The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
     */
    private static final String INPUT_PARAMETER_AVSC_PATH = "Avro schema path";

    /**
     * The Avro Schema as JSON text.
     * The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
     */
    private static final String INPUT_PARAMETER_AVSC_JSON = "Avro schema JSON";

    private static final String SCHEMA_PARAMETER_AVRO_FILE_PATH = "avroFilepath";


    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HOST_IP,
                "Namenode IP, e.g., 192.168.1.3 ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PORT,
                "Namenode port, e.g., 8020 ", true,
                CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_PATH,
                "Path to the Avro schema file. One of these parameters: '"
                    + INPUT_PARAMETER_AVSC_PATH + "' or '"
                    + INPUT_PARAMETER_AVSC_JSON
                    + "' must be specified", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_JSON,
                "JSON of the Avro schema. One of these parameters: '"
                    + INPUT_PARAMETER_AVSC_PATH + "' or '" + INPUT_PARAMETER_AVSC_JSON
                    + "' must be specified", false,
                CustomWrapperInputParameterTypeFactory.stringType()) };


    public HDFSAvroFileWrapper() {
        super();
    }

    /**
     * Defines the input parameters of the Custom Wrapper
     */
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    /**
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
     */
    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        if (logger.isDebugEnabled()) {
            logger.debug("Generating schema for the custom wrapper: " + this.getClass());
            logger.debug("Input parameters: ");
            for (Entry<String, String> inputParam : inputValues.entrySet()) {
                logger.debug(String.format("%s : %s", inputParam.getKey(), inputParam.getValue()));
            }
        }

        String dataNodeIP = inputValues.get(ParameterNaming.HOST_IP);
        String dataNodePort = inputValues.get(ParameterNaming.HOST_PORT);
        Schema avroSchema = obtainAvroSchema(dataNodeIP, dataNodePort);
        if (avroSchema == null) {
            logger.error("Error generating base view schema: the avro schema is null");
            throw new CustomWrapperException("Error generating base view schema: the avro schema is null");
        }

        return obtainVDPSchema(avroSchema);

    }

    private Schema obtainAvroSchema(String dataNodeIP, String dataNodePort) throws CustomWrapperException {

        FSDataInputStream dataInputStream = null;
        try {
            Schema schema = null;

            // The two input parameters AVSC_FILE_PATH and AVSC_JSON are mutually exclusive.
            CustomWrapperInputParameterValue avscFilePathParamValue = getInputParameterValue(INPUT_PARAMETER_AVSC_PATH);
            if (avscFilePathParamValue != null && StringUtils.isNotBlank((String) avscFilePathParamValue.getValue())) {
                String avscFilePath = (String) avscFilePathParamValue.getValue();
                Configuration conf = HadoopConfigurationUtils.getConfiguration(dataNodeIP,
                    dataNodePort);
                Path avscPath = new Path(avscFilePath);
                FileSystem fileSystem = FileSystem.get(conf);
                dataInputStream = fileSystem.open(avscPath);
                schema = new Schema.Parser().parse(dataInputStream);

            } else {
                CustomWrapperInputParameterValue avscJSONParamValue = getInputParameterValue(INPUT_PARAMETER_AVSC_JSON);
                if (avscJSONParamValue != null && StringUtils.isNotBlank((String) avscJSONParamValue.getValue())) {
                    String avscJSON = (String) avscJSONParamValue.getValue();
                    schema = new Schema.Parser().parse(avscJSON);
                } else {
                    throw new CustomWrapperException("One of these parameters: '"
                        + INPUT_PARAMETER_AVSC_PATH + "' or '" + INPUT_PARAMETER_AVSC_JSON
                        + "' must be specified");
                }
            }
            return schema;

        } catch (Exception e) {
            logger.error("Error getting Avro Schema", e);
            throw new CustomWrapperException("Error getting Avro Schema: " + e.getMessage(), e);
        } finally {
            IOUtils.closeStream(dataInputStream);
        }

    }

    private static CustomWrapperSchemaParameter[] obtainVDPSchema(Schema avroSchema) throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        CustomWrapperSchemaParameter avroFilePathParameter = new CustomWrapperSchemaParameter(
            SCHEMA_PARAMETER_AVRO_FILE_PATH, java.sql.Types.VARCHAR,
            null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
            !isUpdateable, !isNullable, isMandatory);

        CustomWrapperSchemaParameter avroSchemaParameter = AvroSchemaUtil.createSchemaParameter(
            avroSchema, avroSchema.getName());

        return new CustomWrapperSchemaParameter[] { avroFilePathParameter, avroSchemaParameter };
    }

    @Override
    public void run(CustomWrapperConditionHolder condition, List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues) throws CustomWrapperException {

        ClassLoader originalCtxClassLoader = changeContextClassLoader();
        HDFSAvroFileReader reader = null;
        try {

            String avroFilePath = getAvroFilePath(condition);
            Path path = new Path(avroFilePath);

            String dataNodeIP = inputValues.get(ParameterNaming.HOST_IP);
            String dataNodePort = inputValues.get(ParameterNaming.HOST_PORT);
            Configuration conf = HadoopConfigurationUtils.getConfiguration(dataNodeIP,
                dataNodePort);

            Schema avroSchema = obtainAvroSchema(dataNodeIP, dataNodePort);
            reader = new HDFSAvroFileReader(path, avroSchema, conf);
            while (reader.hasNext()) {
                Object avroData = reader.read();
                Object[] vdpRow = buildVDPRow(avroFilePath, avroData);
                result.addRow(vdpRow, projectedFields);
            }

        } catch (IOException ie) {
            logger.error("Error accessing Avro file", ie);
            throw new InternalErrorException("Error accessing Avro file: " + ie.getMessage(), ie);

        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error closing the reader", e);
            }
            restoreContextClassLoader(originalCtxClassLoader);
        }
    }

    private static String getAvroFilePath(CustomWrapperConditionHolder condition) {

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition.getConditionMap();
        String avroFilePath = null;
        if (conditionMap != null) {
            for (Entry <CustomWrapperFieldExpression, Object> entry : conditionMap.entrySet()) {
                CustomWrapperFieldExpression field = entry.getKey();
                Object value = entry.getValue();
                if (field.getName().equals(SCHEMA_PARAMETER_AVRO_FILE_PATH)) {
                    avroFilePath = (String) value;
                }
            }
        }
        return avroFilePath;
    }


    private static Object[] buildVDPRow(String avroFilePath, Object avroData) {

        Object[] rowData = new Object[2];
        rowData[0] = avroFilePath;
        rowData[1] = avroData;

        return rowData;
    }

    private static ClassLoader changeContextClassLoader() {
        // Due to getContextClassLoader returning the platform classloader,
        // we need to modify it in order to allow Avro fetch certain classes
        ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
        return originalCtxClassLoader;
    }

    private static void restoreContextClassLoader(ClassLoader originalCtxClassLoader) {
        Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
    }
}
