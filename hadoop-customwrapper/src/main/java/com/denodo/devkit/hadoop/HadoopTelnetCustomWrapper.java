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

import java.io.InputStream;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.DeleteFileException;
import com.denodo.devkit.hadoop.commons.naming.ParameterNaming;
import com.denodo.devkit.hadoop.util.HadoopUtils;
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

public class HadoopTelnetCustomWrapper 
        extends AbstractCustomWrapper {

    
    private static final Logger logger = Logger.getLogger(HadoopTelnetCustomWrapper.class);

    
	private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = 
        new CustomWrapperInputParameter[] {
			new CustomWrapperInputParameter(ParameterNaming.HOST_IP, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_PORT, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_USER, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_PASSWORD, true),
			new CustomWrapperInputParameter(ParameterNaming.HOST_TIMEOUT, true),
			new CustomWrapperInputParameter(ParameterNaming.PATH_TO_JAR_IN_HOST, true),
			new CustomWrapperInputParameter(ParameterNaming.MAIN_CLASS_IN_JAR, true),
            new CustomWrapperInputParameter(ParameterNaming.DATANODE_IP, true),
            new CustomWrapperInputParameter(ParameterNaming.DATANODE_PORT, true),
            new CustomWrapperInputParameter(ParameterNaming.JOBTRACKER_IP, true),
            new CustomWrapperInputParameter(ParameterNaming.JOBTRACKER_PORT, true),
            new CustomWrapperInputParameter(ParameterNaming.INPUT_FILE_PATH, true),
            new CustomWrapperInputParameter(ParameterNaming.DELETE_OUTPUT_FILE, false),
            new CustomWrapperInputParameter(ParameterNaming.MAPREDUCE_CUSTOM_PARAMETERS, false)
        };

    
    
    public HadoopTelnetCustomWrapper() {
        super();
    }
    
    
    
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
    	return INPUT_PARAMETERS;
    }
    
    
    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(Map<String, String> inputValues) 
            throws CustomWrapperException {

    	final CustomWrapperSchemaParameter[] parameters = 
            new CustomWrapperSchemaParameter[] {
        		new CustomWrapperSchemaParameter(
        				ParameterNaming.HADOOP_KEY, 
        				Types.VARCHAR,
        				null,     // complex columns
        				false,    // searchable
        				CustomWrapperSchemaParameter.NOT_SORTABLE, // sortable status
        				false,    // updateable
        				false,    // nullable
        				false),   //mandatory
                new CustomWrapperSchemaParameter(
                		ParameterNaming.HADOOP_VALUE, 
                		Types.INTEGER,
                		null,     // complex columns
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
    		logger.debug("Starting run...");	
    		
    		logger.debug("HOST_IP: " + inputValues.get(ParameterNaming.HOST_IP));
    		logger.debug("HOST_PORT: " + inputValues.get(ParameterNaming.HOST_PORT));
    		logger.debug("HOST_USER: " + inputValues.get(ParameterNaming.HOST_USER));
    		logger.debug("HOST_PASSWORD: " + inputValues.get(ParameterNaming.HOST_PASSWORD));
    		logger.debug("HOST_TIMEOUT: " + inputValues.get(ParameterNaming.HOST_TIMEOUT));
    		
    		logger.debug("PATH_TO_JAR_IN_HOST: " + inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST));
    		logger.debug("MAIN_CLASS_IN_JAR: " + inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR));
    		
    		logger.debug("DATANODE_IP: " + inputValues.get(ParameterNaming.DATANODE_IP));
    		logger.debug("DATANODE_PORT: " + inputValues.get(ParameterNaming.DATANODE_PORT));
    		logger.debug("JOBTRACKER_IP: " + inputValues.get(ParameterNaming.JOBTRACKER_IP));
    		logger.debug("JOBTRACKER_PORT: " + inputValues.get(ParameterNaming.JOBTRACKER_PORT));
    		
    		logger.debug("INPUT_FILE_PATH: " + inputValues.get(ParameterNaming.INPUT_FILE_PATH));
    		logger.debug("DELETE_OUTPUT_FILE: " + inputValues.get(ParameterNaming.DELETE_OUTPUT_FILE));
    		logger.debug("MAPREDUCE_CUSTOM_PARAMETERS: " + inputValues.get(ParameterNaming.MAPREDUCE_CUSTOM_PARAMETERS));
    	
    		logger.debug("Classloader previous to change");
    		logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader());
    		logger.debug("Configuration classloader: " + Configuration.class.getClassLoader());
    		logger.debug("Classloader End");

    	}
    	
    	// Due to getContextClassLoader returning the  platform classloader, we need to modify it in order to allow
    	//hadoop fetch certain classes -it uses getContextClassLoader
    	ClassLoader originalCtxClassLoader = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());
    	
    	if (logger.isDebugEnabled()) {
    		logger.debug("Classloader");
    		logger.debug("Context classloader: " + Thread.currentThread().getContextClassLoader());
    		logger.debug("Configuration classloader: " + Configuration.class.getClassLoader());
    		logger.debug("Classloader End");
    	}
        
    	try {

    		String executionId = RandomStringUtils.randomAlphabetic(5) + "_" + System.nanoTime();  

    		//Output Path (absolute path..it will be created in the root)
    		String outputDir = "/denodo_output_" + executionId;
    		Path outputPath = new Path(outputDir);
    		logger.debug("Output path: " + outputDir);
    		
    		JSch jsch=new JSch();
    		Session session=jsch.getSession(inputValues.get(ParameterNaming.HOST_USER), 
    				inputValues.get(ParameterNaming.HOST_IP), Integer.parseInt(inputValues.get(ParameterNaming.HOST_PORT)));
    		session.setPassword(inputValues.get(ParameterNaming.HOST_PASSWORD));

    		java.util.Properties config = new java.util.Properties(); 
    		config.put("StrictHostKeyChecking", "no");
    		session.setConfig(config);

    		logger.debug("Going to open channel...");
    		    		
    		session.connect(Integer.parseInt(inputValues.get(ParameterNaming.HOST_TIMEOUT)));   // connection with timeout.
    		Channel channel = session.openChannel("exec");

    		// Set command to be executed
    		((ChannelExec)channel).setCommand(HadoopUtils.getCommandToExecuteMapReduceTask(inputValues, outputDir));

    		channel.setInputStream(null);
    		//TODO Modify outputstream??
    		((ChannelExec)channel).setErrStream(System.err);

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

    		channel.disconnect();
    		session.disconnect();
    		
    		// Process output
    		logger.debug("Processing output...");
    		List<Object[]> rows = HadoopUtils.getRows(inputValues, outputPath, projectedFields);
    		for (Object[] row : rows) {
				result.addRow(row, projectedFields);
			}
    		
    		// Delete output folder
    		HadoopUtils.deleteFileIfNecessary(inputValues, outputPath);
    		
    	} catch (Exception e) {
    		logger.error("There has been an error", e);
    		if (e instanceof DeleteFileException) {
    			throw (DeleteFileException) e;
    		}
    		throw new CustomWrapperException("There has been an error", e);
    	}
    	
    	
    	Thread.currentThread().setContextClassLoader(originalCtxClassLoader);
    	
    	logger.debug("Run finished");    	
    }
}
