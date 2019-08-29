package com.denodo.connect.hadoop.hdfs.wrapper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.AbstractHDFSKeyValueFileReader;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;
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
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterRouteValue;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;


/**
 * An abstract base class for a generic HDFS file custom wrapper that
 * reads key value files stored in HDFS (Hadoop Distributed File System).
 *
 */
public abstract class AbstractHDFSKeyValueFileWrapper extends AbstractSecureHadoopWrapper {

    private static final String NUMBER_OF_INVALID_ROWS = "Number of invalid rows";


    private static final  Logger LOG = LoggerFactory.getLogger(AbstractHDFSKeyValueFileWrapper.class); 
    

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(Parameter.FILESYSTEM_URI,
                "e.g. hdfs://<ip>:<port> or s3n://<id>:<secret>\\@<bucket> ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_PATH,
                "Absolute path for the file or the directory containing the files ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(Parameter.FILE_NAME_PATTERN,
                    "Regular expression to filter file names. Example: (.*)\\.csv ", false,
                    CustomWrapperInputParameterTypeFactory.stringType()),            
            new CustomWrapperInputParameter(Parameter.DELETE_AFTER_READING,
                "Delete the file/s after reading? ", true,
                CustomWrapperInputParameterTypeFactory.booleanType(false)),
            new CustomWrapperInputParameter(Parameter.CORE_SITE_PATH,
                "Local route of core-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.HDFS_SITE_PATH,
                "Local route of hdfs-site.xml configuration file ",
                false,  CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL, RouteType.HTTP, RouteType.FTP})),
            new CustomWrapperInputParameter(Parameter.INCLUDE_PATH_COLUMN,
                "Include full path of the file in the view? ", false,
                CustomWrapperInputParameterTypeFactory.booleanType(false))
    };
    

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return (CustomWrapperInputParameter[]) ArrayUtils.addAll(doGetInputParameters(), super.getInputParameters());
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] doGetSchemaParameters(final Map<String, String> inputValues)
        throws CustomWrapperException {

        final String keyHadoopClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_KEY_CLASS));
        final String valueHadoopClass = TypeUtils.getHadoopClass(inputValues.get(Parameter.HADOOP_VALUE_CLASS));
        final boolean includePathColumn = Boolean.parseBoolean(inputValues.get(Parameter.INCLUDE_PATH_COLUMN));
        final Collection<SchemaElement> javaSchema =
            AbstractHDFSKeyValueFileReader.getSchema(keyHadoopClass, valueHadoopClass);
        if(includePathColumn){
            final CustomWrapperSchemaParameter filePath = new CustomWrapperSchemaParameter(Parameter.FULL_PATH, Types.VARCHAR, null, false,
                CustomWrapperSchemaParameter.NOT_SORTABLE, false, true, false);
            return (CustomWrapperSchemaParameter[]) ArrayUtils.add(VDPSchemaUtils.buildSchema(javaSchema),filePath);
        }else {
            return VDPSchemaUtils.buildSchema(javaSchema);
        }
    }

    @Override
    public void doRun(final CustomWrapperConditionHolder condition,
        final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues)
        throws CustomWrapperException {

        final boolean delete = Boolean.parseBoolean(inputValues.get(Parameter.DELETE_AFTER_READING));

        int invalidRows = 0;
        HDFSFileReader reader = null;
        try {

            reader = getHDFSFileReader(inputValues, false);
            Object data = reader.read();

            while (data != null && !isStopRequested()) {
                
                 Object[] row = (Object[]) data;
                int rowLength = row.length;
                if (rowLength != projectedFields.size()) {
                    invalidRows ++;
                    if (!ignoreMatchingErrors(inputValues)) {
                        throw new IllegalArgumentException("Data does not match the schema: line with different number of columns");
                    }
                }

                result.addRow(row, projectedFields);

                data = reader.read();
            }
            

            if (delete) {
                reader.delete();
            }
        } catch (final Exception e) {
            LOG.error("Error accessing HDFS file", e);
            throw new CustomWrapperException("Error accessing HDFS file: " + e.getMessage(), e);
        } finally {

            getCustomWrapperPlan().addPlanEntry(NUMBER_OF_INVALID_ROWS, String.valueOf(invalidRows));
            try {
                if (reader != null && !delete) {
                    reader.close();
                }
            } catch (final IOException e) {
                LOG.error("Error releasing the reader", e);
            }

        }
    }

    public boolean ignoreMatchingErrors(final Map<String, String> inputValues) {
        return false;
    }

    public CustomWrapperInputParameter[] doGetInputParameters() {
        return INPUT_PARAMETERS;
    }

    public abstract HDFSFileReader getHDFSFileReader(Map<String, String> inputValues, boolean getSchemaParameters)
        throws IOException, InterruptedException, CustomWrapperException;


}
