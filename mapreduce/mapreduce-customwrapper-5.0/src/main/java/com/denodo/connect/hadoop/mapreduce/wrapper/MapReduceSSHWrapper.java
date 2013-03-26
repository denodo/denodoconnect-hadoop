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
import java.io.InputStream;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.hdfs.util.classloader.ClassLoaderUtils;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.handler.IMapReduceTaskHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.output.IMapReduceTaskOutputReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.util.MapReduceUtils;
import com.denodo.connect.hadoop.mapreduce.wrapper.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class MapReduceSSHWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger
        .getLogger(MapReduceSSHWrapper.class);

    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS =
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(ParameterNaming.HOST_IP, "IP ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PORT,
                "SSH port, default is 22 ", true,
                CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_USER,
                "Username ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_PASSWORD,
                "Password ", true,
                CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(ParameterNaming.HOST_TIMEOUT,
                "Connection timeout ", true,
                CustomWrapperInputParameterTypeFactory.longType()),
            new CustomWrapperInputParameter(
                ParameterNaming.PATH_TO_JAR_IN_HOST,
                "Path to locate the jar with the MapReduce task ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(
                ParameterNaming.MAIN_CLASS_IN_JAR,
                "Class in the jar with a main method that will execute the MapReduce task ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_KEY_CLASS,
                "Hadoop Key class ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_VALUE_CLASS,
                "Hadoop Value class ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(
                ParameterNaming.IMAPREDUCETASKHANDLER_IMPLEMENTATION,
                "Class implementing IMapReduceTaskHandler ", true,
                CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(
                ParameterNaming.MAPREDUCE_PARAMETERS,
                "Parameters for the MapReduce task ", false,
                CustomWrapperInputParameterTypeFactory.stringType()) };

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
        Map<String, String> inputValues) throws CustomWrapperException {

        boolean isSearchable = true;
        boolean isUpdateable = true;
        boolean isNullable = true;
        boolean isMandatory = true;

        int keyType = TypeUtils.getSqlType(inputValues
            .get(ParameterNaming.HADOOP_KEY_CLASS));
        int valueType = TypeUtils.getSqlType(inputValues
            .get(ParameterNaming.HADOOP_VALUE_CLASS));

        final CustomWrapperSchemaParameter[] parameters =
            new CustomWrapperSchemaParameter[] {
                new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_KEY,
                    keyType, null, !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    !isUpdateable, !isNullable, !isMandatory),
                new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_VALUE,
                    valueType,
                    valueType == Types.ARRAY ? new CustomWrapperSchemaParameter[] { new CustomWrapperSchemaParameter(
                        ParameterNaming.HADOOP_VALUE, TypeUtils.getSqlType(StringUtils.substringBeforeLast(
                        inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS), "[]"))) }
                        : null,
                    !isSearchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                    !isUpdateable, !isNullable, !isMandatory),
            };

        return parameters;
    }

    @Override
    public void run(CustomWrapperConditionHolder condition,
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues)
        throws CustomWrapperException {

        if (logger.isDebugEnabled()) {
            logger.debug("Starting run...");

            logger.debug("HOST_IP: " + inputValues.get(ParameterNaming.HOST_IP));
            logger.debug("HOST_PORT: " + inputValues.get(ParameterNaming.HOST_PORT));
            logger.debug("HOST_USER: " + inputValues.get(ParameterNaming.HOST_USER));
            logger.debug("HOST_TIMEOUT: " + inputValues.get(ParameterNaming.HOST_TIMEOUT));
            logger.debug("PATH_TO_JAR_IN_HOST: " + inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST));
            logger.debug("MAIN_CLASS_IN_JAR: " + inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR));
            logger.debug("HADOOP_KEY_TYPE: " + inputValues.get(ParameterNaming.HADOOP_KEY_CLASS));
            logger.debug("HADOOP_VALUE_TYPE: " + inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS));
            logger.debug("MAPREDUCEPARAMETERS: " + inputValues.get(ParameterNaming.MAPREDUCE_PARAMETERS));

        }

        ClassLoader originalCtxClassLoader = ClassLoaderUtils.changeContextClassLoader();
        try {

            IMapReduceTaskHandler mapReduceTaskHandler = (IMapReduceTaskHandler) Class.forName(inputValues
                .get(ParameterNaming.IMAPREDUCETASKHANDLER_IMPLEMENTATION))
                .newInstance();

            int exitStatus = executeMapReduceTask(inputValues, mapReduceTaskHandler);

            // If ok -> process output
            if (exitStatus == 0) {
                processMapReduceOutput(projectedFields, result, inputValues,
                    mapReduceTaskHandler);
            } else {
                throw new IOException("Exit status returned '" + exitStatus
                    + "'. You may set logging  to debug in order to see shell output");
            }

        } catch (Exception e) {
            logger.error("Error executing MapReduce task", e);
            throw new CustomWrapperException("Error executing MapReduce task: " + e.getMessage(), e);
        } finally {
            ClassLoaderUtils.restoreContextClassLoader(originalCtxClassLoader);
        }

        logger.debug("Run finished");
    }

    private static int executeMapReduceTask(Map<String, String> inputValues,
        IMapReduceTaskHandler mapReduceTaskHandler) throws JSchException, IOException {

        Session session = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(inputValues.get(ParameterNaming.HOST_USER),
                inputValues.get(ParameterNaming.HOST_IP), Integer.parseInt(inputValues.get(ParameterNaming.HOST_PORT)));
            session.setPassword(inputValues.get(ParameterNaming.HOST_PASSWORD));

            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            logger.debug("Going to open channel...");
            session.connect(Integer.parseInt(inputValues
                .get(ParameterNaming.HOST_TIMEOUT))); // connection with
            // timeout.
            Channel channel = session.openChannel("exec");

            // Set command to be executed
            ((ChannelExec) channel).setCommand(MapReduceUtils
                .getCommandToExecuteMapReduceTask(inputValues, mapReduceTaskHandler));

            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            channel.connect(Integer.parseInt(inputValues.get(ParameterNaming.HOST_TIMEOUT)));

            if (logger.isDebugEnabled()) {
                logger.debug("*** Shell output ***");
                byte[] tmp = new byte[1024];
                while (true) {
                    while (in.available() > 0) {
                        int i = in.read(tmp, 0, 1024);
                        if (i < 0) {
                            break;
                        }
                        logger.debug(new String(tmp, 0, i));
                    }
                    if (channel.isClosed()) {
                        logger.debug("exit-status: " + channel.getExitStatus());
                        break;
                    }
                }
                logger.debug("*** Shell output end ***");
            }

            int exitStatus = channel.getExitStatus();
            logger.debug("Exit status: " + exitStatus);


            return exitStatus;
        } finally {
            if (session != null) {
                session.disconnect();
            }
        }

    }

    private static void processMapReduceOutput(
        List<CustomWrapperFieldExpression> projectedFields,
        CustomWrapperResult result, Map<String, String> inputValues,
        IMapReduceTaskHandler hadoopTaskHandler) throws IOException {

        IMapReduceTaskOutputReader reader = null;
        try {
            String hostIp = inputValues.get(ParameterNaming.HOST_IP);
            String hostPort = inputValues.get(ParameterNaming.HOST_PORT);
            String hostUser = inputValues.get(ParameterNaming.HOST_USER);
            String hostPassword = inputValues.get(ParameterNaming.HOST_PASSWORD);
            String hostTimeout = inputValues.get(ParameterNaming.HOST_TIMEOUT);
            String pathToJarInHost = inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST);
            String mainClassInJar = inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR);
            String hadoopKeyClass = inputValues.get(ParameterNaming.HADOOP_KEY_CLASS);
            String hadoopValueClass = inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS);
            String mapReduceParameters = inputValues.get(ParameterNaming.MAPREDUCE_PARAMETERS);

            logger.debug("Processing output...");
            reader = hadoopTaskHandler.getOutputReader(
                hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost,
                mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters);
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

        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }


}
