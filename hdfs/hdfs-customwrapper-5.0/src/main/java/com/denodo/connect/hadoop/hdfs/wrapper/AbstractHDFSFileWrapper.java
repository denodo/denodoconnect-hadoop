package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.reader.HDFSKeyValueReader;
import com.denodo.connect.hadoop.hdfs.util.classloader.ClassLoaderUtils;
import com.denodo.connect.hadoop.hdfs.wrapper.commons.naming.ParameterNaming;
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
 * An abstract base class for a generic HDFS file custom wrapper that
 * reads files stored in HDFS (Hadoop Distributed File System).
 *
 */
public abstract class AbstractHDFSFileWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(AbstractHDFSFileWrapper.class);

    private static final CustomWrapperInputParameter[] COMMON_INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.FILESYSTEM_URI,
                "e.g. hdfs://ip:port or s3n://id:secret\\@bucket ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
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

        ClassLoader originalCtxClassLoader = ClassLoaderUtils.changeContextClassLoader();
        HDFSKeyValueReader reader = null;
        try {

            String hadoopKeyClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_KEY_CLASS);
            String hadoopValueClass = getHadoopClass(inputValues, ParameterNaming.HADOOP_VALUE_CLASS);

            // Process file
            logger.debug("Processing file...");
            reader = getHDFSFileReader(inputValues);
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
        } catch (Exception e) {
            logger.error("Error accessing HDFS file", e);
            throw new CustomWrapperException("Error accessing HDFS file: " + e.getMessage(), e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error closing the reader", e);
            }
            ClassLoaderUtils.restoreContextClassLoader(originalCtxClassLoader);
        }
    }

    protected static String getHadoopClass(Map<String, String> inputValues, String key) {

        String hadoopClass = inputValues.get(key);
        return (hadoopClass != null) ? hadoopClass : Text.class.getName();
    }

    public abstract HDFSKeyValueReader getHDFSFileReader(Map<String, String> inputValues)
        throws IOException;

    public abstract CustomWrapperInputParameter[] getSpecificInputParameters();

}
