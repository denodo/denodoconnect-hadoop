package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.AbstractHDFSKeyValueFileReader;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;


/**
 * An abstract base class for a generic HDFS file custom wrapper that
 * reads key value files stored in HDFS (Hadoop Distributed File System).
 *
 */
public abstract class AbstractHDFSKeyValueFileWrapper extends AbstractSecureHadoopWrapper {

    private static final Logger logger = Logger.getLogger(AbstractHDFSKeyValueFileWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\@<bucket> ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_PATH,
                "Absolute path for the file or the directory containing the files ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING,
                "Delete the file/s after reading? ", true,
                CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL}))                     
    };
    

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(doGetInputParameters(), super.getInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(Map<String, String> inputValues)
        throws CustomWrapperException {

        String keyHadoopClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_KEY_CLASS));
        String valueHadoopClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_VALUE_CLASS));

        Collection<SchemaElement> javaSchema =
            AbstractHDFSKeyValueFileReader.getSchema(keyHadoopClass, valueHadoopClass);

        return VDPSchemaUtils.buildSchema(javaSchema);

    }

    @Override
    public void doRun(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        HDFSFileReader reader = null;
        try {

            reader = getHDFSFileReader(inputValues);
            Object data = reader.read();
            while (data != null && !isStopRequested()) {
                result.addRow((Object[]) data, projectedFields);

                data = reader.read();
            }

            if (delete) {
                reader.delete();
            }
        } catch (Exception e) {
            logger.error("Error accessing HDFS file", e);
            throw new CustomWrapperException("Error accessing HDFS file: " + e.getMessage(), e);
        } finally {

            try {
                if (reader != null && !delete) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Error releasing the reader", e);
            }

        }
    }

    public CustomWrapperInputParameter[] doGetInputParameters() {
        return INPUT_PARAMETERS;
    }

    public abstract HDFSFileReader getHDFSFileReader(Map<String, String> inputValues)
        throws IOException, InterruptedException;


}
