package com.denodo.devkit.hadoop.util;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.InternalErrorException;
import com.denodo.devkit.hadoop.util.configuration.HadoopConfigurationUtils;

public class HadoopReadOutputUtils {

	private static final Logger logger = Logger
            .getLogger(HadoopReadOutputUtils.class);
	
	public static LinkedHashMap<Writable, Writable> getRowsFromSequenceFileOutputFormat(Path outputPath,
	        String dataNodeIp, String dataNodePort, String hadoopKeyClass, String hadoopValueClass) {
		
	    LinkedHashMap<Writable, Writable> results = new LinkedHashMap<Writable, Writable>();
		
		SequenceFile.Reader reader = null;
		
		try {
			Configuration configuration = HadoopConfigurationUtils.getConfiguration(dataNodeIp, dataNodePort, hadoopKeyClass, hadoopValueClass);
			FileSystem fileSystem = FileSystem.get(configuration);

			if (logger.isDebugEnabled()) {
			    logger.debug("FileSystem is: " + fileSystem.getUri());
			    logger.debug("Output path is: " + outputPath);
			}

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
									
					while (reader.next(key, value)) {
					    //TODO If we avoided clone by returning final data instead of converting it later...
					    results.put(WritableUtils.clone(key, configuration), WritableUtils.clone(value, configuration));
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
	
	
	//TODO Test and complete this if necessary
	public static LinkedHashMap<WritableComparable<?>, Writable> getRowsFromMapFileOutputFormat(Path outputPath,
            String dataNodeIp, String dataNodePort, String hadoopKeyClass, String hadoopValueClass) {
        
        LinkedHashMap<WritableComparable<?>, Writable> results = new LinkedHashMap<WritableComparable<?>, Writable>();
        
        MapFile.Reader reader = null;
        
        try {
            Configuration configuration = HadoopConfigurationUtils.getConfiguration(dataNodeIp, dataNodePort, hadoopKeyClass, hadoopValueClass);
            FileSystem fileSystem = FileSystem.get(configuration);

            if (logger.isDebugEnabled()) {
                logger.debug("FileSystem is: " + fileSystem.getUri());
                logger.debug("Output path is: " + outputPath);
            }

            FileStatus[] fss = fileSystem.listStatus(outputPath);
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (logger.isDebugEnabled()) {
                    logger.debug("Reading path: " + path.getName());
                }
                if (!status.isDirectory()) {                                        
                    reader = new MapFile.Reader(path, configuration);

                    WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
                                    
                    while (reader.next(key, value)) {
                        //TODO If we avoided clone by returning final data instead of converting it later...
                        results.put(WritableUtils.clone(key, configuration), WritableUtils.clone(value, configuration));
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
