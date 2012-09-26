package com.denodo.devkit.hdfs.wrapper;

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

import com.denodo.devkit.hdfs.wrapper.util.AvroSchemaUtil;
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

    private static final String SCHEMA_PARAMETER_AVRO_FILE_PATH = "avro_filepath";

    /*
     * Stores the wrapper's schema
     */
    private CustomWrapperSchemaParameter[] schema = null;
    private Schema avro_schema = null;

    public HdfsAvroFileWrapper() {
        super();
        // Due to getContextClassLoader returning the platform classloader, we
        // need to modify it in order to allow
        // Hadoop and Avro fetch certain classes -it uses getContextClassLoader
        Thread.currentThread().setContextClassLoader(
                Configuration.class.getClassLoader());
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
                new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_FILE_PATH,
                        "Path to the Avsc file", false,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INPUT_PARAMETER_USE_AVSC_FILE,
                        "Use the Avsc file for creating the schema?", true,
                        CustomWrapperInputParameterTypeFactory
                                .booleanType(true)),
                new CustomWrapperInputParameter(INPUT_PARAMETER_AVSC_SCHEMA,
                        "JSON definition of the Avro schema", false,
                        CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(
                        INPUT_PARAMETER_DELETE_AFTER_READING,
                        "Delete file after reading it?", true,
                        CustomWrapperInputParameterTypeFactory
                                .booleanType(true)) };
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
        configuration
                .setAllowedOperators(new String[] { CustomWrapperCondition.OPERATOR_EQ });
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
    public CustomWrapperSchemaParameter[] getSchemaParameters(
            Map<String, String> inputValues) throws CustomWrapperException {
        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        if (this.schema != null) {
            return this.schema;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Generating schema for custom wrapper: "
                    + this.getClass());
            logger.debug("Input parameters: ");
            for (Entry<String, String> inputParam : inputValues.entrySet()) {
                logger.debug(String.format("%s : %s", inputParam.getKey(),
                        inputParam.getValue()));
            }
        }
        boolean use_avsc_file = (Boolean) getInputParameterValue(
                INPUT_PARAMETER_USE_AVSC_FILE).getValue();
        if (use_avsc_file) {
            String host = (String) getInputParameterValue(
                    INPUT_PARAMETER_NAMENODE_HOST).getValue();
            int port = (Integer) getInputParameterValue(
                    INPUT_PARAMETER_NAMENODE_PORT).getValue();
            String avsc_file_path = (String) getInputParameterValue(
                    INPUT_PARAMETER_AVSC_FILE_PATH).getValue();

            if (logger.isDebugEnabled()) {
                logger.debug("Accesing Avro Schema at: " + "hdfs://" + host
                        + ":" + port + "\n File path: " + avsc_file_path);
            }
            Configuration conf = new Configuration();
            // TO AVOID WEIRD "No FileSystem for scheme: hdfs" EXCEPTION
            conf.set("fs.hdfs.impl",
                    "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.default.name", "hdfs://" + host + ":" + port);

            Path avsc_path = new Path(avsc_file_path);
            FileSystem fileSystem;
            FSDataInputStream dataInputStream = null;
            try {
                fileSystem = FileSystem.get(conf);
                dataInputStream = fileSystem.open(avsc_path);
                this.avro_schema = Schema.parse(dataInputStream);
            } catch (IOException e) {
                String stackTrace = getStacktraceAsString(e);
                logger.error("Error getting Avro Schema: " + stackTrace, e);
                throw new CustomWrapperException("Error getting Avro Schema: "
                        + stackTrace, e);
            } catch (Exception ee) {
                String stackTrace = getStacktraceAsString(ee);
                logger.error(
                        "Error generating base view schema: " + stackTrace, ee);
                throw new CustomWrapperException(
                        "Error generating base view schema: " + stackTrace, ee);
            }
        } else {
            String avsc_schema = (String) getInputParameterValue(
                    INPUT_PARAMETER_AVSC_SCHEMA).getValue();
            if (avsc_schema != null)
                this.avro_schema = Schema.parse(avsc_schema);
        }
        if (this.avro_schema != null) {

            CustomWrapperSchemaParameter avro_schema_parameter = AvroSchemaUtil
                    .createSchemaParameter(this.avro_schema,
                            this.avro_schema.getName());
            this.schema = new CustomWrapperSchemaParameter[] {
                    new CustomWrapperSchemaParameter(
                            SCHEMA_PARAMETER_AVRO_FILE_PATH,
                            java.sql.Types.VARCHAR, null, !isSearchable,
                            CustomWrapperSchemaParameter.NOT_SORTABLE,
                            !isUpdateable, !isNullable, isMandatory),
                    avro_schema_parameter };
            return this.schema;
        } else {
            logger.error("Error generating base view. Avro schema is null");
            throw new CustomWrapperException(
                    "Error generating base view. Avro schema is null");
        }
    }

    public void run(CustomWrapperConditionHolder condition,
            List<CustomWrapperFieldExpression> projectedFields,
            CustomWrapperResult result, Map<String, String> inputValues)
            throws CustomWrapperException {
        // FIXME this.schema is null inside this method .... !?¿?!¿!!?¿!?¿?¿?¿
        this.schema = getSchemaParameters(inputValues);

        String host = (String) getInputParameterValue(
                INPUT_PARAMETER_NAMENODE_HOST).getValue();
        int port = (Integer) getInputParameterValue(
                INPUT_PARAMETER_NAMENODE_PORT).getValue();

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);
        // TO AVOID WEIRD "No FileSystem for scheme: hdfs" EXCEPTION
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Map<CustomWrapperFieldExpression, Object> conditionMap = condition
                .getConditionMap();
        String avro_file_path = "";
        if (conditionMap != null) {
            for (CustomWrapperFieldExpression field : conditionMap.keySet()) {
                Object value = conditionMap.get(field);
                if (field.getName().equals(SCHEMA_PARAMETER_AVRO_FILE_PATH)) {
                    avro_file_path = (String) value;
                }
            }
        } else

        if (logger.isDebugEnabled()) {
            logger.debug("Reading Avro file: " + avro_file_path + " in "
                    + " hdfs://" + host + ":" + port);
            logger.debug("Using Avro schema: " + this.avro_schema);
        }
        // File to read
        Path path = new Path(avro_file_path);
        try {
            FsInput inputFile = new FsInput(path, conf);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(
                    this.avro_schema);
            DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<GenericData.Record>(
                    inputFile, reader);
            // Build the output object
            // FIXME Only GenericData.Record supported. Try GenericData.Array
            // FIXME Try GenericData.Fixed and GenericData.EnumSymbol
            if (this.schema != null) {
                for (GenericData.Record avro_record : dataFileReader) {
                    Object[] rowData = new Object[this.schema.length];
                    rowData[0] = avro_file_path;
                    int i = 1;
                    // FIXME This only goes one level deep in the schema, it has
                    // to go to the deepest level
                    // FIXME Until no schema parameter with columns is found
                    for (CustomWrapperSchemaParameter schema_param : this.schema) {
                        if (!schema_param.getName().equalsIgnoreCase(
                                SCHEMA_PARAMETER_AVRO_FILE_PATH)) {
                            if (schema_param.getColumns().length > 0) {
                                Object[] cols = new Object[schema_param
                                        .getColumns().length];
                                int j = 0;
                                for (CustomWrapperSchemaParameter schema_cols : schema_param
                                        .getColumns()) {
                                    cols[j++] = ((GenericData.Record) avro_record)
                                            .get(schema_cols.getName());
                                }
                                rowData[i++] = cols;
                            } else
                                rowData[i++] = ((GenericData.Record) avro_record)
                                        .get(schema_param.getName());
                        }
                    }
                    result.addRow(rowData, projectedFields);
                }
            } else {
                logger.error("This should never happen. Schema is null");
                throw new CustomWrapperException(
                        "his should never happen. Schema is null");
            }
        } catch (IOException ie) {
            String stackTrace = getStacktraceAsString(ie);
            logger.error("Error accesing Avro file: " + stackTrace);
            throw new CustomWrapperException("Error accesing Avro file: "
                    + stackTrace, ie);

        } catch (Exception e) {
            String stackTrace = getStacktraceAsString(e);
            logger.error("Error runing custom wrapper: " + stackTrace);
            throw new CustomWrapperException("Error runing custom wrapper: "
                    + stackTrace, e);
        }
    }

    private static String getStacktraceAsString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        return stackTrace;
    }
}
