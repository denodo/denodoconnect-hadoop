package com.denodo.devkit.hadoop.util.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.log4j.Logger;

public class HadoopConfigurationUtils {

    private static final Logger logger = Logger
            .getLogger(HadoopConfigurationUtils.class);
    
    
    /**
     * 
     * @param inputValues
     * @return the basic hadoop configuration (only including datanode ip and port) 
     */
    public static Configuration getConfiguration(String dataNodeIp, String dataNodePort, String hadoopKeyClass, String hadoopValueClass) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" 
                + dataNodeIp + ":" 
                + dataNodePort);
        conf.set(JobContext.OUTPUT_KEY_CLASS, hadoopKeyClass);
        conf.set(JobContext.OUTPUT_VALUE_CLASS, hadoopValueClass);
        //TODO Check jobtracker is not necessary (mapred.job.tracker)
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //Remove SUCESS file from output dir
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
        logger.debug("Returning configuration: " + conf
                + " - value of 'fs.default.name' -> " + conf.get("fs.default.name")
                + " - value of 'fs.hdfs.impl' -> " + conf.get("fs.hdfs.impl")
                + " - value of 'mapreduce.fileoutputcommitter.marksuccessfuljobs' -> " + conf.get("mapreduce.fileoutputcommitter.marksuccessfuljobs"));
        return conf;
    }
    
}
