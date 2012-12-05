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
package com.denodo.connect.hadoop.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.denodo.connect.hadoop.commons.exception.DeleteFileException;
import com.denodo.connect.hadoop.commons.handler.IHadoopTaskHandler;
import com.denodo.connect.hadoop.commons.naming.ParameterNaming;
import com.denodo.connect.hadoop.util.configuration.HadoopConfigurationUtils;

public class HadoopUtils {

	private static final Logger logger = Logger
            .getLogger(HadoopUtils.class);
	
	/**
	 * It deletes the given file or folder
	 * 
	 */
	public static void deleteFile(String dataNodeIp, String dataNodePort,
	        String hadoopKeyClass, String hadoopValueClass, Path outputPath) {
	    try {
	        logger.debug("Deleting... '" + outputPath + "'"); //$NON-NLS-1$ //$NON-NLS-2$
	        Configuration configuration = HadoopConfigurationUtils.getConfiguration(
	                dataNodeIp, dataNodePort, 
	                hadoopKeyClass, hadoopValueClass);
	        FileSystem.get(configuration).delete(outputPath, true);
	    } catch (IOException e) {
	        throw new DeleteFileException(outputPath);
	    }
	}
				
					
	
	
	/**
	 * It returns the command to be executed. Something like:
	 * hadoop jar PATH_TO_JAR_IN_HOST MAIN_CLASS_IN_JAR DATANODE_IP DATANODE_PORT
	 * JOBTRACKER_IP JOBTRACKER_PORT INPUT_FILE_PATH OUTPUT_FILE_PATH
	 * (i.e.  hadoop jar /home/cloudera/ssanchez/jars/hadooptestwordcount-1.0-SNAPSHOT.jar test.MapReduceDriver1 
	 * 172.16.0.58 8020 172.16.0.58 8021 /user/cloudera/input /user/cloudera/output
	 *  
	 * The parameters to be added to "hadoop jar PATH_TO_JAR_IN_HOST MAIN_CLASS_IN_JAR"
	 * come from {@link IHadoopTaskHandler#getMapReduceParameters(String, String, String, String, String, String, String, String, String, String)}
	 * @param inputValues
	 * @return
	 */
	public static String getCommandToExecuteMapReduceTask(Map<String, String> inputValues, 
	        IHadoopTaskHandler hadoopTaskHandler) {
		StringBuffer output = new StringBuffer("hadoop jar "); //$NON-NLS-1$
		output.append(inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST));
		output.append(" "); //$NON-NLS-1$
		output.append(inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR));
		
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
		String[] parameters = hadoopTaskHandler.getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword, 
                hostTimeout, pathToJarInHost, mainClassInJar, 
                hadoopKeyClass, hadoopValueClass, mapReduceParameters);
		
		for (String param : parameters) {
		    output.append(" "); //$NON-NLS-1$
		    output.append(param);
        }		
		
		if (logger.isDebugEnabled()) {
			logger.debug("Returning command: " + output.toString()); //$NON-NLS-1$
		}
		
		return output.toString();
	}
	
}
