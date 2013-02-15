package com.denodo.connect.hadoop.hdfs;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.connect.hadoop.commons.result.text.TextFileOutputFormatHadoopResultIterator;
import com.denodo.connect.hadoop.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
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
 * You will be asked Namenode host, Namenode port, file path and file separator.
 * <br/>
 * If everything works fine, the key-value pairs contained in the file will be
 * returned by the wrapper
 * </p>
 * 
 * @see AbstractCustomWrapper
 */
public class HdfsDelimitedTextFileWrapper extends AbstractCustomWrapper {
    private static final Logger logger = Logger.getLogger(HdfsDelimitedTextFileWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HOST_IP, "Namenode hostname or IP, e.g., localhost or 192.168.1.3 ", true,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PORT, "Namenode port, e.g., 8020 ", true,
                    CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(ParameterNaming.INPUT_FILE_PATH, "Input path for the file(s)  ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.SEPARATOR, "Separator of the delimited file(s) ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.DELETE_AFTER_READING, "Delete file after reading it?", true,
                    CustomWrapperInputParameterTypeFactory.booleanType(false)) };

    private CustomWrapperSchemaParameter[] schema;

    public HdfsDelimitedTextFileWrapper() {
        super();
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues) throws CustomWrapperException {
        boolean isSearchable = true;
        boolean isUpdeatable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        if (this.schema != null) {
            return this.schema;
        }
        this.schema = new CustomWrapperSchemaParameter[] {
                new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_KEY, java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdeatable, isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_VALUE, java.sql.Types.VARCHAR, null, !isSearchable,
                        CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdeatable, isNullable, !isMandatory) };
        return this.schema;

    }

    @Override
    public void run(CustomWrapperConditionHolder condition, List<CustomWrapperFieldExpression> projectedFields, CustomWrapperResult result,
            Map<String, String> inputValues) throws CustomWrapperException {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting run..."); //$NON-NLS-1$
            logger.debug("HOST_IP: " + inputValues.get(ParameterNaming.HOST_IP)); //$NON-NLS-1$
            logger.debug("HOST_PORT: " + inputValues.get(ParameterNaming.HOST_PORT)); //$NON-NLS-1$
            logger.debug("INPUT_FILE_PATH: " + inputValues.get(ParameterNaming.INPUT_FILE_PATH)); //$NON-NLS-1$
            logger.debug("SEPARATOR: " + inputValues.get(ParameterNaming.SEPARATOR)); //$NON-NLS-1$
            logger.debug("DELETE_AFTER_READING: " + inputValues.get(ParameterNaming.DELETE_AFTER_READING)); //$NON-NLS-1$

            logger.debug("Classloader previous to change"); //$NON-NLS-1$
            logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
            logger.debug("Configuration classloader: " + Configuration.class.getClassLoader()); //$NON-NLS-1$
            logger.debug("Classloader End"); //$NON-NLS-1$
        }
        // Due to getContextClassLoader returning the platform classloader,
        // we need to modify it in order to allow
        // hadoop fetch certain classes -it uses getContextClassLoader
        ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());

        if (logger.isDebugEnabled()) {
            logger.debug("Classloader"); //$NON-NLS-1$
            logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
            logger.debug("Configuration classloader: " + Configuration.class.getClassLoader()); //$NON-NLS-1$
            logger.debug("Classloader End"); //$NON-NLS-1$
        }

        String hostIp = inputValues.get(ParameterNaming.HOST_IP);
        String hostPort = inputValues.get(ParameterNaming.HOST_PORT);
        String inputFilePath = inputValues.get(ParameterNaming.INPUT_FILE_PATH);
        String separator = inputValues.get(ParameterNaming.SEPARATOR);
        Path path = new Path(inputFilePath);
        boolean deleteOutputPathAfterReadOutput = Boolean.parseBoolean(inputValues.get(ParameterNaming.DELETE_AFTER_READING));
        String hadoopKeyClass = Text.class.getName();
        String hadoopValueClass = Text.class.getName();

        // Process file
        logger.debug("Processing file..."); //$NON-NLS-1$
        IHadoopResultIterator resultIterator = new TextFileOutputFormatHadoopResultIterator(hostIp, hostPort, separator, path,
                deleteOutputPathAfterReadOutput);
        Writable key = resultIterator.getInitKey();
        Writable value = resultIterator.getInitValue();
        while (resultIterator.readNext(key, value)) {
            Object[] asArray = new Object[projectedFields.size()];
            if (projectedFields.get(0).getName().equalsIgnoreCase(ParameterNaming.HADOOP_KEY)) {
                asArray[0] = TypeUtils.getValue(hadoopKeyClass, key);
            }
            if (projectedFields.get(0).getName().equalsIgnoreCase(ParameterNaming.HADOOP_VALUE)) {
                asArray[0] = TypeUtils.getValue(hadoopValueClass, value);
            }
            if (projectedFields.size() == 2) {
                if (projectedFields.get(1).getName().equalsIgnoreCase(ParameterNaming.HADOOP_KEY)) {
                    asArray[1] = TypeUtils.getValue(hadoopKeyClass, key);
                }
                if (projectedFields.get(1).getName().equalsIgnoreCase(ParameterNaming.HADOOP_VALUE)) {
                    asArray[1] = TypeUtils.getValue(hadoopValueClass, value);
                }
            }
            result.addRow(asArray, projectedFields);
        }
        Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
        logger.debug("Run finished"); //$NON-NLS-1$
    }
}
