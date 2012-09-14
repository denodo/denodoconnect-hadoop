/**
 * @(#)
 *
 * Copyright (c) 2005. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 */

package com.denodo.devkit.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.DeleteFileException;
import com.denodo.devkit.hadoop.commons.handler.IHadoopTaskHandler;
import com.denodo.devkit.hadoop.commons.naming.ParameterNaming;
import com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.devkit.hadoop.util.HadoopUtils;
import com.denodo.devkit.hadoop.util.type.TypeUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class HadoopSSHCustomWrapper 
        extends AbstractCustomWrapper {

    
    private static final Logger logger = Logger.getLogger(HadoopSSHCustomWrapper.class);

    
	private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = 
        new CustomWrapperInputParameter[] {
			new CustomWrapperInputParameter(ParameterNaming.HOST_IP, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_PORT, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_USER, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_PASSWORD, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_TIMEOUT, true),
			new CustomWrapperInputParameter(ParameterNaming.PATH_TO_JAR_IN_HOST, true),
			new CustomWrapperInputParameter(ParameterNaming.MAIN_CLASS_IN_JAR, true),
			new CustomWrapperInputParameter(ParameterNaming.HADOOP_KEY_CLASS, true),
            new CustomWrapperInputParameter(ParameterNaming.HADOOP_VALUE_CLASS, true),
            new CustomWrapperInputParameter(ParameterNaming.CLASS_IMPLEMENTING_IHADOOPTASKHANDLER, true),
            new CustomWrapperInputParameter(ParameterNaming.MAPREDUCE_PARAMETERS, false)
        };

    
    
    public HadoopSSHCustomWrapper() {
        super();
    }
    
    
    
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
    	return INPUT_PARAMETERS;
    }
    
    
    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues) 
            throws CustomWrapperException {
        
        int keyType = TypeUtils.getSqlType(inputValues.get(ParameterNaming.HADOOP_KEY_CLASS));
        int valueType = TypeUtils.getSqlType(inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS));
        
    	final CustomWrapperSchemaParameter[] parameters = 
            new CustomWrapperSchemaParameter[] {
        		new CustomWrapperSchemaParameter(
        				ParameterNaming.HADOOP_KEY, 
        				keyType,
        				null,     // complex columns
        				false,    // searchable
        				CustomWrapperSchemaParameter.NOT_SORTABLE, // sortable status
        				false,    // updateable
        				false,    // nullable
        				false),   //mandatory
                new CustomWrapperSchemaParameter(
                		ParameterNaming.HADOOP_VALUE, 
                		valueType,
                		valueType == Types.ARRAY 
                		    ? new CustomWrapperSchemaParameter[] {
                                    new CustomWrapperSchemaParameter(ParameterNaming.HADOOP_VALUE, 
                                            TypeUtils.getSqlType(StringUtils.substringBeforeLast(inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS), "[]"))) //$NON-NLS-1$
                            } 
                		    : null,     // complex columns
                		false,    // searchable
                		CustomWrapperSchemaParameter.NOT_SORTABLE, // sortable status
                		false,    // updateable
                		false,    // nullable
                		false),   //mandatory                        
           };
        
        return parameters;
    }

    
    @Override
    public void run(CustomWrapperConditionHolder condition, 
            List<CustomWrapperFieldExpression> projectedFields, 
            CustomWrapperResult result, 
            Map<String, String> inputValues) 
            throws CustomWrapperException {
    	
    	if (logger.isDebugEnabled()) {
    		logger.debug("Starting run...");	 //$NON-NLS-1$
    		
    		logger.debug("HOST_IP: " + inputValues.get(ParameterNaming.HOST_IP)); //$NON-NLS-1$
    		logger.debug("HOST_PORT: " + inputValues.get(ParameterNaming.HOST_PORT)); //$NON-NLS-1$
    		logger.debug("HOST_USER: " + inputValues.get(ParameterNaming.HOST_USER)); //$NON-NLS-1$
    		logger.debug("HOST_PASSWORD: " + inputValues.get(ParameterNaming.HOST_PASSWORD)); //$NON-NLS-1$
    		logger.debug("HOST_TIMEOUT: " + inputValues.get(ParameterNaming.HOST_TIMEOUT)); //$NON-NLS-1$
    		
    		logger.debug("PATH_TO_JAR_IN_HOST: " + inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST)); //$NON-NLS-1$
    		logger.debug("MAIN_CLASS_IN_JAR: " + inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR)); //$NON-NLS-1$
    		
    		logger.debug("HADOOP_KEY_TYPE: " + inputValues.get(ParameterNaming.HADOOP_KEY_CLASS)); //$NON-NLS-1$
    		logger.debug("HADOOP_VALUE_TYPE: " + inputValues.get(ParameterNaming.HADOOP_VALUE_CLASS)); //$NON-NLS-1$
    		logger.debug("MAPREDUCEPARAMETERS: " + inputValues.get(ParameterNaming.MAPREDUCE_PARAMETERS)); //$NON-NLS-1$
    	
    		logger.debug("Classloader previous to change"); //$NON-NLS-1$
    		logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
    		logger.debug("Configuration classloader: " + Configuration.class.getClassLoader()); //$NON-NLS-1$
    		logger.debug("Classloader End"); //$NON-NLS-1$

    	}
    	
    	// Due to getContextClassLoader returning the  platform classloader, we need to modify it in order to allow
    	//hadoop fetch certain classes -it uses getContextClassLoader
    	ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
    	
    	if (logger.isDebugEnabled()) {
    		logger.debug("Classloader"); //$NON-NLS-1$
    		logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
    		logger.debug("Configuration classloader: " + Configuration.class.getClassLoader()); //$NON-NLS-1$
    		logger.debug("Classloader End"); //$NON-NLS-1$
    	}
        
    	InputStream in = null;
    	try {

    		IHadoopTaskHandler hadoopTaskHandler = (IHadoopTaskHandler) Class.forName(inputValues
    		        .get(ParameterNaming.CLASS_IMPLEMENTING_IHADOOPTASKHANDLER)).newInstance();
    		
    		JSch jsch=new JSch();
    		Session session=jsch.getSession(inputValues.get(ParameterNaming.HOST_USER), 
    				inputValues.get(ParameterNaming.HOST_IP), Integer.parseInt(inputValues.get(ParameterNaming.HOST_PORT)));
    		session.setPassword(inputValues.get(ParameterNaming.HOST_PASSWORD));

    		java.util.Properties config = new java.util.Properties(); 
    		config.put("StrictHostKeyChecking", "no"); //$NON-NLS-1$ //$NON-NLS-2$
    		session.setConfig(config);

    		logger.debug("Going to open channel..."); //$NON-NLS-1$
    		    		
    		session.connect(Integer.parseInt(inputValues.get(ParameterNaming.HOST_TIMEOUT)));   // connection with timeout.
    		Channel channel = session.openChannel("exec"); //$NON-NLS-1$

    		// Set command to be executed
    		((ChannelExec)channel).setCommand(HadoopUtils.getCommandToExecuteMapReduceTask(inputValues, hadoopTaskHandler));

    		channel.setInputStream(null);
    		((ChannelExec)channel).setErrStream(System.err);

    		in = channel.getInputStream();
    		channel.connect(Integer.parseInt(inputValues.get(ParameterNaming.HOST_TIMEOUT)));

    		if (logger.isDebugEnabled()) {
    			logger.debug("*** Shell output ***"); //$NON-NLS-1$
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
    					logger.debug("exit-status: " + channel.getExitStatus()); //$NON-NLS-1$
    					break;
    				}
    			}
    			logger.debug("*** Shell output end ***"); //$NON-NLS-1$
    		}

    		int exitStatus = channel.getExitStatus();
    		logger.debug("Exit status: " + exitStatus); //$NON-NLS-1$
    		
    		channel.disconnect();
    		session.disconnect();
    		
    		// If ok -> process output
    		if (exitStatus == 0) {
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
    	        
    		    // Process output
                logger.debug("Processing output..."); //$NON-NLS-1$
                                
                IHadoopResultIterator resultIterator = hadoopTaskHandler.getResultIterator(hostIp, hostPort, hostUser, hostPassword, hostTimeout, 
                        pathToJarInHost, mainClassInJar, 
                        hadoopKeyClass, hadoopValueClass, mapReduceParameters);
                Writable key = resultIterator.getInitKey();
                Writable value = resultIterator.getInitValue();
                while (resultIterator.readNext(key, value)){
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
                in.close();
    		} else {    
    		    in.close();
    		    throw new CustomWrapperException("Exit status returned '" + exitStatus //$NON-NLS-1$
    		            + "'. You may set logging  to debug in order to see shell output"); //$NON-NLS-1$
    		}		
    	} catch (Exception e) {
    	    if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    logger.warn("Error closing inputStream", e1); //$NON-NLS-1$
                }
            }
    		logger.error("There has been an error", e); //$NON-NLS-1$
    		if (e instanceof DeleteFileException) {
    			throw (DeleteFileException) e;
    		}
    		if (e instanceof CustomWrapperException) {
                throw (CustomWrapperException) e;
            }
    		throw new CustomWrapperException("There has been an error", e); //$NON-NLS-1$
    	} finally {
    	    if (in != null) {
    	        try {
    	            in.close();
    	        } catch (IOException e) {
    	            logger.warn("Error closing inputStream", e); //$NON-NLS-1$
    	        }
    	    }
    	}
    	
    	Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
    	
    	logger.debug("Run finished");    	 //$NON-NLS-1$
    }
}
