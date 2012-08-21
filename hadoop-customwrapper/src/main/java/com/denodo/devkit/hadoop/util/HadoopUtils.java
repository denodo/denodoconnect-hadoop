package com.denodo.devkit.hadoop.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.configuration.IHadoopTaskHandler;
import com.denodo.devkit.hadoop.commons.exception.DeleteFileException;
import com.denodo.devkit.hadoop.commons.naming.ParameterNaming;
import com.denodo.devkit.hadoop.util.configuration.HadoopConfigurationUtils;

public class HadoopUtils {

	private static final Logger logger = Logger
            .getLogger(HadoopUtils.class);
	
	/**
	 * It deletes the given folder 
	 * 
	 */
	public static void deleteFile(String dataNodeIp, String dataNodePort,
	        String hadoopKeyClass, String hadoopValueClass, Path outputPath) {
	    try {
	        logger.debug("Deleting... '" + outputPath + "'");
	        Configuration configuration = HadoopConfigurationUtils.getConfiguration(dataNodeIp, dataNodePort, hadoopKeyClass, hadoopValueClass);
	        DistributedFileSystem.get(configuration).delete(outputPath, true);
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
	 * @param inputValues
	 * @return
	 */
	public static String getCommandToExecuteMapReduceTask(Map<String, String> inputValues, 
	        IHadoopTaskHandler hadoopTaskHandler) {
		StringBuffer output = new StringBuffer("hadoop jar ");
		output.append(inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST));
		output.append(" ");
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
		
		
		
		for (String param : hadoopTaskHandler.getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword, 
		        hostTimeout, pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters)) {
		    output.append(" ");
		    output.append(param);
        }
		
		
		if (logger.isDebugEnabled()) {
			logger.debug("Returning command: " + output.toString());
		}
		
		return output.toString();
	}
	
}
