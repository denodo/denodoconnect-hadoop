package com.denodo.devkit.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.DeleteFileException;
import com.denodo.devkit.hadoop.commons.exception.InternalErrorException;
import com.denodo.devkit.hadoop.commons.exception.UnsupportedProjectionException;
import com.denodo.devkit.hadoop.commons.naming.ParameterNaming;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class HadoopUtils {

	private static final Logger logger = Logger
            .getLogger(HadoopUtils.class);
	
	/**
	 * It deletes the given file in case value of inputValue 
	 * {@link ParameterNaming#DELETE_OUTPUT_FILE} is not present
	 * or {@link BooleanUtils#isTrue(Boolean)} returns true for its value
	 */
	public static void deleteFileIfNecessary(Map<String, String> inputValues, Path outputPath) {
		if (StringUtils.isBlank(inputValues.get(ParameterNaming.DELETE_OUTPUT_FILE))
				|| BooleanUtils.toBoolean(inputValues.get(ParameterNaming.DELETE_OUTPUT_FILE))) {
			
			try {
				Configuration conf = getConfiguration(inputValues);
				DistributedFileSystem.get(conf).delete(outputPath, true);
			} catch (IOException e) {
				throw new DeleteFileException(outputPath);
			}
		}
	}
				
					
	/**
	 * 
	 * @param inputValues
	 * @return the basic hadoop configuration (only including datanode ip and port) 
	 */
	public static Configuration getConfiguration(Map<String, String> inputValues) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://" 
				+ inputValues.get(ParameterNaming.DATANODE_IP) + ":" 
				+ inputValues.get(ParameterNaming.DATANODE_PORT));
		conf.set("mapred.job.tracker", inputValues.get(ParameterNaming.JOBTRACKER_IP)  + ":" 
				+ inputValues.get(ParameterNaming.JOBTRACKER_PORT));
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
	    //Remove SUCESS file from output dir
	    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
		logger.debug("Returning configuration: " + conf
				+ " - value of 'fs.default.name' -> " + conf.get("fs.default.name")
				+ " - value of 'mapred.job.tracker' -> " + conf.get("mapred.job.tracker")
				+ " - value of 'fs.hdfs.impl' -> " + conf.get("fs.hdfs.impl")
				+ " - value of 'mapreduce.fileoutputcommitter.marksuccessfuljobs' -> " + conf.get("mapreduce.fileoutputcommitter.marksuccessfuljobs"));
		return conf;
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
	public static String getCommandToExecuteMapReduceTask(Map<String, String> inputValues, String outputDir) {
		StringBuffer output = new StringBuffer("hadoop jar ");
		output.append(inputValues.get(ParameterNaming.PATH_TO_JAR_IN_HOST));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.MAIN_CLASS_IN_JAR));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.DATANODE_IP));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.DATANODE_PORT));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.JOBTRACKER_IP));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.JOBTRACKER_PORT));
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.INPUT_FILE_PATH));
		output.append(" ");
		output.append(outputDir);
		output.append(" ");
		output.append(inputValues.get(ParameterNaming.MAPREDUCE_CUSTOM_PARAMETERS));
		
		if (logger.isDebugEnabled()) {
			logger.debug("Returning command: " + output.toString());
		}
		
		return output.toString();
	}
	
	
	
	/**
	 * It extracts the rows data from the output directory.
	 * 
	 * @param inputValues
	 * @param outputPath
	 * @param projectedFields
	 * @return a list with the rows where every row contains the columns key and/or value
	 * if they are present in the projectedFields 
	 */
	public static List<Object[]> getRows(Map<String, String> inputValues, Path outputPath,
			List<CustomWrapperFieldExpression> projectedFields) {
		
		List<Object[]> results = new ArrayList<Object[]>();
		
		SequenceFile.Reader reader = null;
		
		try {
			Configuration configuration = getConfiguration(inputValues);
			FileSystem fileSystem = FileSystem.get(configuration);

			
			logger.debug("FileSystem is: " + fileSystem.getUri());
			logger.debug("Output path is: " + outputPath);
			
			FileStatus[] fss = fileSystem.listStatus(outputPath);
			for (FileStatus status : fss) {
				Path path = status.getPath();
				if (logger.isDebugEnabled()) {
					logger.debug("Reading path: " + path.getName());
				}
				if (!status.isDirectory()) {										
					reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path));

					Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
					Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

					//Actually only supports retrieving key/value
					if (projectedFields.size() > 2 || projectedFields.size() < 1) {
						throw new UnsupportedProjectionException("projectedFields must have more than 0 and less than 3 elements");
					}

					boolean fetchValue = projectedFields.size() == 2 || projectedFields.get(0).getName().equalsIgnoreCase(ParameterNaming.HADOOP_VALUE);
					while (fetchValue ? reader.next(key, value) : reader.next(key)) {
						List<Object> row = new ArrayList<Object>();
						if (projectedFields.get(0).getName().equalsIgnoreCase(ParameterNaming.HADOOP_KEY)) {
							row.add(key.toString());
						}
						if (projectedFields.get(0).getName().equalsIgnoreCase(ParameterNaming.HADOOP_VALUE)) {
							row.add(value.toString());
						}
						if (projectedFields.size() == 2) {
							if (projectedFields.get(1).getName().equalsIgnoreCase(ParameterNaming.HADOOP_KEY)) {
								row.add(key.toString());
							}
							if (projectedFields.get(1).getName().equalsIgnoreCase(ParameterNaming.HADOOP_VALUE)) {
								row.add(value.toString());
							}	
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Row -> " + row);
						}
						results.add(row.toArray(new Object[row.size()]));
					}
					reader.close();
				}
			}
		} catch (Exception e) {
			throw new InternalErrorException("There has been an error reading results from output file " + outputPath, e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// Do nothing
				}
			}
		}
		
		return results;
	}
	
}
