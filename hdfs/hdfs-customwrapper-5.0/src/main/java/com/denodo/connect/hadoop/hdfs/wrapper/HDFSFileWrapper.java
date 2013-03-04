package com.denodo.connect.hadoop.hdfs.wrapper;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.hdfs.wrapper.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.wrapper.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;


/**
 * An abstract base class for a generic HDFS File Connector Custom Wrapper.
 * For reading files stored in HDFS (Hadoop Distributed File System)
 * <p>
 *
 * If everything works fine, the key-value pairs contained in the file will be
 * returned by the wrapper
 * </p>
 *
 */
public abstract class HDFSFileWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(HDFSFileWrapper.class);

    private static final CustomWrapperInputParameter[] COMMON_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HOST_IP,
                "Namenode IP, e.g., 192.168.1.3 ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PORT,
                "Namenode port, e.g., 8020 ", true,
                CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(ParameterNaming.INPUT_FILE_PATH,
                "Input path for the file or the directory containing the files ", true,
                CustomWrapperInputParameterTypeFactory.stringType())};


    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(COMMON_INPUT_PARAMETERS, getSpecificInputParameters());
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        String keyHadoopClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_KEY_CLASS);
        int keyType = TypeUtils.getSqlType(keyHadoopClass);
        String valueHadoopClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_VALUE_CLASS);
        int valueType = TypeUtils.getSqlType(valueHadoopClass);

        return new CustomWrapperSchemaParameter[] {
            new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_KEY,
                keyType, null, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable,
                isNullable, !isMandatory),
            new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_VALUE,
                valueType, null, !isSearchable,
                CustomWrapperSchemaParameter.NOT_SORTABLE, !isUpdateable,
                isNullable, !isMandatory) };

    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        ClassLoader originalCtxClassLoader = changeContextClassLoader();
        try {

            String hadoopKeyClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_KEY_CLASS);
            String hadoopValueClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_VALUE_CLASS);

            // Process file
            logger.debug("Processing file...");
            HDFSFileReader reader = getHDFSFileReader(inputValues);
            Writable key = reader.getInitKey();
            Writable value = reader.getInitValue();
            while (reader.readNext(key, value)) {
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
            logger.debug("Run finished");
        } finally {
            restoreContextClassLoader(originalCtxClassLoader);
        }
    }

    protected static String getHadoopClass(Map<String, String> inputValues, String key) {

        String hadoopClass = inputValues.get(key);
        return (hadoopClass != null) ? hadoopClass : Text.class.getName();
    }

    private static ClassLoader changeContextClassLoader() {
        // Due to getContextClassLoader returning the platform classloader,
        // we need to modify it in order to allow Hadoop fetch certain classes
        ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
        return originalCtxClassLoader;
    }

    private static void restoreContextClassLoader(ClassLoader originalCtxClassLoader) {
        Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
    }

    public abstract HDFSFileReader getHDFSFileReader(Map<String, String> inputValues);

    public abstract CustomWrapperInputParameter[] getSpecificInputParameters();

}
