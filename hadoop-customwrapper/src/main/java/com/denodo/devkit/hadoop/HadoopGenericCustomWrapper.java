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
import java.rmi.UnexpectedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.WordCount.TokenCounterMapper;
import com.denodo.devkit.hadoop.WordCount.TokenCounterReducer;
import com.denodo.devkit.hadoop.exceptions.CustomParametersParsingException;
import com.denodo.devkit.hadoop.exceptions.UnsupportedProjectionException;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class HadoopGenericCustomWrapper 
        extends AbstractCustomWrapper {

    
    private static final Logger logger = Logger.getLogger(HadoopGenericCustomWrapper.class);

    
    
	//Parameters
	public static String DATANODE_IP = "Datanode IP";
	public static String DATANODE_PORT = "Datanode port";
	public static String JOBTRACKER_IP = "Jobtracker IP";
	public static String JOBTRACKER_PORT = "Jobtracker port";
	public static String INPUT_FILE_PATH = "Input file path";
	
	//Hadoop custom parameters syntax: 
	//key:value pairs divided by "," characters
	//Both key and value can be escaped by surrounded each one with either " or ' characters
	public static String HADOOP_CUSTOM_PARAMETERS = "Custom parameters";
	
	public static String HADOOP_KEY = "word";
	public static String HADOOP_VALUE = "count";
	
	
	//Projection cases
	
	public enum KeyValueProjection {
        KEY, VALUE, KEY_VALUE, VALUE_KEY
    };
	
	
    private static final CustomWrapperInputParameter[] INPUT_PARAMETERS = 
        new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(DATANODE_IP, true),
            new CustomWrapperInputParameter(DATANODE_PORT, true),
            new CustomWrapperInputParameter(JOBTRACKER_IP, true),
            new CustomWrapperInputParameter(JOBTRACKER_PORT, true),
            new CustomWrapperInputParameter(INPUT_FILE_PATH, true),
            new CustomWrapperInputParameter(HADOOP_CUSTOM_PARAMETERS, false)
        };

    
    
    public HadoopGenericCustomWrapper() {
        super();
    }
    
    
    
    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return INPUT_PARAMETERS;
    }
    
    

    @Override
    public CustomWrapperConfiguration getConfiguration() {
        return super.getConfiguration();
    }
    
    
    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(final Map<String, String> inputValues) 
            throws CustomWrapperException {

        final CustomWrapperSchemaParameter[] parameters = 
            new CustomWrapperSchemaParameter[] {
        
                new CustomWrapperSchemaParameter(
                        HADOOP_KEY, 
                        Types.VARCHAR,
                        null,     // complex columns
                        false,    // searchable
                        CustomWrapperSchemaParameter.NOT_SORTABLE, // sortable status
                        false,    // updateable
                        false,     // nullable
                        false),     //mandatory
                        
               new CustomWrapperSchemaParameter(
                                HADOOP_VALUE, 
                                Types.INTEGER,
                                null,     // complex columns
                                false,    // searchable
                                CustomWrapperSchemaParameter.NOT_SORTABLE, // sortable status
                                false,    // updateable
                                false,     // nullable
                                false),   //mandatory                        
           };
        
        return parameters;
    }

    
	@Override
    public void run(final CustomWrapperConditionHolder condition, 
            final List<CustomWrapperFieldExpression> projectedFields, 
            final CustomWrapperResult result, 
            final Map<String, String> inputValues) 
            throws CustomWrapperException {
    	
    	
    		String executionId = RandomStringUtils.randomAlphabetic(8);  
    		String outputDir = "denodo_output"+executionId;
    	
        	//Establishing job configuration
    	    Configuration conf = new Configuration();
    	    //HDFS IP & port
    	    conf.set("fs.default.name", "hdfs://"+inputValues.get(DATANODE_IP)+":"+inputValues.get(DATANODE_PORT));
    	    //Jobtracker IP & Port
    	    conf.set("mapred.job.tracker",inputValues.get(JOBTRACKER_IP)+":"+inputValues.get(JOBTRACKER_PORT));
    	    //Remove SUCESS file from output dir
    	    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");

    	    //Custom parameters parsing
    	    if (inputValues.get(HADOOP_CUSTOM_PARAMETERS)!= null) {
    	    	Map<String,String> customParameters = calculateCustomParametersMap(splitCustomParameters(inputValues.get(HADOOP_CUSTOM_PARAMETERS)));
    	    	for (String key : customParameters.keySet()) {
    	    		conf.set(key,customParameters.get(key));
				}
    	    }
//    	    conf.set("input.selectedword", inputValues.get(WORD_TO_COUNT));
    	    //Output Path
    	    Path outputPath = new Path(outputDir);
    	    SequenceFile.Reader reader  = null; 
    	    
    	    
    	    try {
    	    
        	FileSystem fileSystem = FileSystem.get(conf);
    	    	    
    	    //Job creation 
    	    Job job = new Job(conf, "Denodo execution: "+executionId);
    	    job.setJarByClass(WordCount.class);
    	    job.setMapperClass(TokenCounterMapper.class);
    	    job.setReducerClass(TokenCounterReducer.class);    
    	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(IntWritable.class);
    	    
    	    //The file path in this case must begin with "../.." because the ssh is not connecting with the same 
    	    //user that has the info we are processing
    	    FileInputFormat.addInputPath(job, new Path(inputValues.get(INPUT_FILE_PATH)));
    	    FileOutputFormat.setOutputPath(job, outputPath);
    	    
    	    //wait for job finishing
    	    boolean complete = job.waitForCompletion(true);
    	    
    	    
    	    // If everything is ok we must read the output and delete it. Hadoop cannot stream the results so we have to wait until
    	    // it finishes, read the output files and delete them
    	    if (complete) {
    		    
    		    FileStatus[] fss = fileSystem.listStatus(outputPath);
    		    for (FileStatus status : fss) {
    		        Path path = status.getPath();
    		        if (!status.isDir()) {
    			        reader = new SequenceFile.Reader(fileSystem, path, conf);
    			        Text key = new Text();
    			        //TODo probar con String
    			        IntWritable value = new IntWritable();
    			        
    			        KeyValueProjection keyValueProjection = null;
    			            			        
    			        //Actually only supports retrieving key/value
    			        if (projectedFields.size()>2 || projectedFields.size()<1) {
    			        	throw new UnsupportedProjectionException("projectedFields must have more than 0 and less than 3 elements");
    			        }
    			        
    			        //Check the first element
    			        String projectedFieldsFirstElementName = projectedFields.get(0).getName();
    			        
    			        if (projectedFieldsFirstElementName.equals(HADOOP_KEY)) {
    			        	keyValueProjection = KeyValueProjection.KEY;    			        	
    			        }else if (projectedFieldsFirstElementName.equals(HADOOP_VALUE)) {
    			        	keyValueProjection = KeyValueProjection.VALUE;
    			        }else {
    			        	throw new UnsupportedProjectionException("Wrong projection: first element is neither "+HADOOP_KEY+" nor "+HADOOP_VALUE);
    			        }
    			        
    			        if (projectedFields.size() == 2) {
    			        	String projectedFieldsSecondElementName = projectedFields.get(1).getName();
    			        	
    			        	if (projectedFieldsSecondElementName.equals(HADOOP_KEY)) {
    			        		if (keyValueProjection.equals(KeyValueProjection.KEY)) {
    			        			throw new UnsupportedProjectionException("Duplicated key element in projectedFields");
    			        		}
    			        		keyValueProjection = KeyValueProjection.VALUE_KEY;
        			        }else if (projectedFieldsSecondElementName.equals(HADOOP_VALUE)) {
        			        	if (keyValueProjection.equals(KeyValueProjection.VALUE)) {
        			        		throw new UnsupportedProjectionException("Duplicated value element in projectedFields");
        			        	}
        			        		keyValueProjection = KeyValueProjection.KEY_VALUE;        			        	
        			        }else {
        			        	throw new UnsupportedProjectionException("Wrong projection: second element is neither "+HADOOP_KEY+" nor "+HADOOP_VALUE);
        			        }
    			        	
    			        }
    			        
    			        
    			        while (keyValueProjection.equals(KeyValueProjection.KEY)?reader.next(key):reader.next(key, value)) {
   			            	
    			        	
    			        	switch(keyValueProjection) {
    			        	
	    			        	case KEY: result.addRow(
	    			        			new Object[] {key.toString()}, 
	    		                        projectedFields); 
	    			        			break;
	    			        	case VALUE: result.addRow(
        		                        new Object[] {Integer.valueOf(value.get())}, 
        		                        projectedFields);
										break;
	    			        	case KEY_VALUE: result.addRow(
        		                        new Object[] {key.toString(), Integer.valueOf(value.get())}, 
        		                        projectedFields);
	    			        			break;
	    			        	case VALUE_KEY: result.addRow(
        		                        new Object[] {Integer.valueOf(value.get()),key.toString()}, 
        		                        projectedFields);
	    			        			break;
	    			        	default: throw new UnexpectedException("Key value can never reach this state at this point");
    			        	}
    			        }
    			        reader.close();
    		        }
    		    }
    		    
    		    
    	    }
        	
            
        } catch (final Exception e) {
            logger.error("Error executing the stored procedure", e);
            throw new CustomWrapperException("Exception while executing hadoop wrapper", e);
        } finally {
        	try {
        		FileSystem fileSystem = FileSystem.get(conf);
				fileSystem.delete(outputPath, true);
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				throw new CustomWrapperException("Exception while executing hadoop wrapper", e);
			}
        }
        
    }
    
    //
    private List<String> splitCustomParameters(String input){
    	List<String> result = new ArrayList<String>();
    	boolean quoteOpened = false;
    	boolean backslashOpened = false;
    	StringBuffer keyValuePair = new StringBuffer();
    	for (char character : input.toCharArray()) {
    		if (character == '\\') {
   				keyValuePair.append(character);
    			backslashOpened = !backslashOpened;
    		}else if (!quoteOpened && character == ',') {
    			result.add(keyValuePair.toString());
    			keyValuePair = new StringBuffer();
    			backslashOpened = false;
    		}else if (character == '"') {
    			if (!backslashOpened) {
    				quoteOpened = !quoteOpened;
    			}
    			keyValuePair.append(character);
    			backslashOpened = false;
    		}else {
    			keyValuePair.append(character);
    			backslashOpened = false;
    		}
		}
    	result.add(keyValuePair.toString());
    	
    	if (quoteOpened) {
    		throw new CustomParametersParsingException("Quotation marks are not correctly closed");
    	}
    	
    	return result;
    }
    
    
    private Map<String,String> calculateCustomParametersMap(List<String> splittedCustomParameters){
    	Map<String,String> customParameters = new HashMap<String, String>();    	
    	for (String customParameter : splittedCustomParameters) {
    		boolean quoteOpened = false;
    		boolean backslashOpened = false;
    		boolean keyValueSeparatorFound = false;
    		String key = null;
    		String value = null;
    		StringBuffer storedCharacters = new StringBuffer();
    		
			for (char character : customParameter.toCharArray()) {
				if (character == '\\') {
					if (backslashOpened) {
						storedCharacters.append(character);
					}
					backslashOpened =!backslashOpened;
				}else if (character == '"') {
					if (!backslashOpened) {
						quoteOpened = !quoteOpened;
					}else {
						storedCharacters.append(character);
					}
					backslashOpened = false;
				}else if (!quoteOpened && character == ':') {
					//We expect only keyValueSeparator
					if (keyValueSeparatorFound) {
						throw new CustomParametersParsingException("More than one key/separator character found outside quotation marks within a key/value pair");
					}
					keyValueSeparatorFound = true;
					key = storedCharacters.toString();
					storedCharacters = new StringBuffer();
					backslashOpened = false;
				}else {
					storedCharacters.append(character);
					backslashOpened = false;
				}
			}
			value = storedCharacters.toString();
			if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
				throw new CustomParametersParsingException("Empty key or value are not allowed");
			}
			customParameters.put(key, value);
		}
    	return customParameters;
    }
    
    
    

}
