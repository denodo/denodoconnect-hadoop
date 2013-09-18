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
package com.denodo.connect.hadoop.mapreduce.wrapper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.classloader.ClassLoaderUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.hadoop.mapreduce.wrapper.handler.AbstractFileOutputMapReduceJobHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.handler.AvroFileOutputMapReduceJobHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.handler.DelimitedFileOutputMapReduceJobHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.handler.KeyValueFileOutputMapReduceJobHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.handler.MapReduceJobHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobOutputReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.util.MapReduceUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory.RouteType;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterLocalRouteValue;
import com.jcraft.jsch.JSchException;

public class MapReduceSSHWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(MapReduceSSHWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = new CustomWrapperInputParameter[] {
        new CustomWrapperInputParameter(Parameter.HOST_IP, "Name of the computer or IP address where Hadoop is running  ",
            true, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.HOST_PORT, "Port number to connect to the Hadoop machine, default is 22 ",
            true, CustomWrapperInputParameterTypeFactory.integerType()),
        new CustomWrapperInputParameter(Parameter.USER, "Username to log into the Hadoop machine ",
            true, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.PASSWORD, "Password associated with the username ",
            false, CustomWrapperInputParameterTypeFactory.passwordType()),
        new CustomWrapperInputParameter(Parameter.KEY_FILE, "Key file created with PuTTY key file generator ",
            false, CustomWrapperInputParameterTypeFactory.routeType(new RouteType [] {RouteType.LOCAL})),
        new CustomWrapperInputParameter(Parameter.PASSPHRASE, "Passphrase associated with the key file ",
            false, CustomWrapperInputParameterTypeFactory.passwordType()),
        new CustomWrapperInputParameter(Parameter.TIMEOUT, "Amount of time, in milliseconds, that the wrapper will wait for the Hadoop command to complete. Configure a value of 0 (zero), or leave the box blank, to wait indefinitely ",
            false, CustomWrapperInputParameterTypeFactory.longType()),
        new CustomWrapperInputParameter(Parameter.PATH_TO_JAR_IN_HOST, "Path to locate the jar with the MapReduce job ",
            true, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter( Parameter.MAIN_CLASS_IN_JAR, "Main class in the jar that will execute the MapReduce job ",
            true, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.MAPREDUCE_PARAMETERS, "Parameters for the MapReduce job ",
            false, CustomWrapperInputParameterTypeFactory.stringType()),
        new CustomWrapperInputParameter(Parameter.MAPREDUCEJOBHANDLER_IMPLEMENTATION, "Class implementing MapReduceJobHandler ",
            true, CustomWrapperInputParameterTypeFactory.stringType())
    };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {

        CustomWrapperInputParameter[] params = (CustomWrapperInputParameter[]) ArrayUtils.addAll(INPUT_PARAMETERS, KeyValueFileOutputMapReduceJobHandler.INPUT_PARAMETERS);
        params = (CustomWrapperInputParameter[]) ArrayUtils.addAll(params, DelimitedFileOutputMapReduceJobHandler.INPUT_PARAMETERS);
        params = (CustomWrapperInputParameter[]) ArrayUtils.addAll(params, AvroFileOutputMapReduceJobHandler.INPUT_PARAMETERS);

        return params;
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration conf = super.getConfiguration();
        conf.setDelegateProjections(false);

        return conf;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
        Map<String, String> inputValues) throws CustomWrapperException {

        String jobHandlerClassName = inputValues.get(Parameter.MAPREDUCEJOBHANDLER_IMPLEMENTATION);
        try {

            MapReduceJobHandler jobHandler = (MapReduceJobHandler) Class.forName(
                jobHandlerClassName).newInstance();

            // Check input here so we can inform the user about errors at base view creation time.
            checkInput(inputValues, jobHandler);

            Collection<SchemaElement> javaSchema = jobHandler.getSchema(inputValues);

            return VDPSchemaUtils.buildSchema(javaSchema);

        } catch (ClassNotFoundException e) {
            logger.error(Parameter.MAPREDUCEJOBHANDLER_IMPLEMENTATION + ":'" + jobHandlerClassName
                + "' not found", e);
            throw new CustomWrapperException(Parameter.MAPREDUCEJOBHANDLER_IMPLEMENTATION + ":'" + jobHandlerClassName
                + "' not found", e);
        } catch (Exception e) {
            logger.error("Error building VDP schema", e);
            throw new CustomWrapperException("Error building VDP schema: " + e.getMessage(), e);
        }

    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        ClassLoader originalCtxClassLoader = ClassLoaderUtils.changeContextClassLoader();

        try {

            MapReduceJobHandler jobHandler = (MapReduceJobHandler) Class.forName(
                inputValues.get(Parameter.MAPREDUCEJOBHANDLER_IMPLEMENTATION)).newInstance();

            executeMapReduceJob(inputValues, jobHandler);
            processMapReduceJobOutput(projectedFields, result, inputValues, jobHandler);

        } catch (Exception e) {
            logger.error("Error executing MapReduce job", e);
            throw new CustomWrapperException("Error executing MapReduce job: " + e.getMessage(), e);
        } finally {

            ClassLoaderUtils.restoreContextClassLoader(originalCtxClassLoader);
        }

    }

    private void executeMapReduceJob(Map<String, String> inputValues,
        MapReduceJobHandler jobHandler) throws JSchException, IOException {

        String host = inputValues.get(Parameter.HOST_IP);
        int port = Integer.parseInt(inputValues.get(Parameter.HOST_PORT));
        String user = inputValues.get(Parameter.USER);
        String password = inputValues.get(Parameter.PASSWORD);
        String keyFile = null;
        if (inputValues.get(Parameter.KEY_FILE) != null) {
            keyFile = ((CustomWrapperInputParameterLocalRouteValue) getInputParameterValue(Parameter.KEY_FILE)).getPath();
        }
        String passphrase = inputValues.get(Parameter.PASSPHRASE);
        int timeout = getTimeout(inputValues.get(Parameter.TIMEOUT));

        String jar = inputValues.get(Parameter.PATH_TO_JAR_IN_HOST);
        String mainClass = inputValues.get(Parameter.MAIN_CLASS_IN_JAR);

        String parameters = inputValues.get(Parameter.MAPREDUCE_PARAMETERS);

        String command = MapReduceUtils.getCommand(jar, mainClass, parameters,
            jobHandler.getJobParameters(inputValues));

        SSHClient sshClient = null;
        if (keyFile != null) {
            sshClient = new SSHClient(host, port, user, timeout, keyFile, passphrase);
        } else {
            sshClient = new SSHClient(host, port, user, timeout);
            sshClient.setPassword(password);
        }

        int exitStatus = sshClient.execute(command);
        if (exitStatus == 0) {
            logger.debug("The standard output of the remote command '" + command
                + "' is '" + sshClient.getExecutionOutput() + "'");
        } else {
            throw new IOException("Exit status returned '" + exitStatus
                + "'. The standard error output of the remote command '" + command
                + "' is '" + sshClient.getExecutionError() + "'");
        }

    }

    private static void processMapReduceJobOutput(List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues,
        MapReduceJobHandler jobHandler) throws IOException {

        MapReduceJobOutputReader reader = null;
        try {

            logger.debug("Processing output...");

            reader = jobHandler.getOutputReader(inputValues);
            Object[] data = reader.read();
            while (data != null) {
                result.addRow(data, projectedFields);

                data = reader.read();
            }

        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static void checkInput(Map<String, String> inputValues, MapReduceJobHandler jobHandler) {

        String password = inputValues.get(Parameter.PASSWORD);
        String keyFile = inputValues.get(Parameter.KEY_FILE);

        if (StringUtils.isBlank(password) && StringUtils.isBlank(keyFile)) {
            throw new IllegalArgumentException("One of these parameters: '"
                + Parameter.PASSWORD + "' or '" + Parameter.KEY_FILE + "' must be specified");
        }

        if (jobHandler instanceof AbstractFileOutputMapReduceJobHandler) {
            ((AbstractFileOutputMapReduceJobHandler) jobHandler).checkInput(inputValues);
        }
    }

    /*
     * By default the timeout is 0, interpreted as an infinite timeout.
     */
    private static int getTimeout(String timeoutAsString) {

        int timeout = 0;
        if (timeoutAsString != null) {
            timeout = Integer.parseInt(timeoutAsString);
        }

        return timeout;
    }

}
