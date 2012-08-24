package com.denodo.devkit.hadoop.util.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;

public class HadoopConfigurationUtils {

    private static final Logger logger = Logger
            .getLogger(HadoopConfigurationUtils.class);
    
    
    /**
     * 
     * @param inputValues
     * @return the basic hadoop configuration (only including datanode ip, port,
     * outputkeyClass and outputValueClass) 
     */
    public static Configuration getConfiguration(String dataNodeIp, String dataNodePort, 
            String hadoopKeyClass, String hadoopValueClass) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://"  //$NON-NLS-1$ //$NON-NLS-2$
                + dataNodeIp + ":"  //$NON-NLS-1$
                + dataNodePort);
        
        conf.set(MRJobConfig.OUTPUT_KEY_CLASS, hadoopKeyClass);
        conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, hadoopValueClass);
        
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); //$NON-NLS-1$ //$NON-NLS-2$
        //Remove SUCESS file from output dir
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false"); //$NON-NLS-1$ //$NON-NLS-2$
        logger.debug("Returning configuration: " + conf //$NON-NLS-1$
                + " - value of 'fs.default.name' -> " + conf.get("fs.default.name") //$NON-NLS-1$ //$NON-NLS-2$
                + " - value of 'fs.hdfs.impl' -> " + conf.get("fs.hdfs.impl") //$NON-NLS-1$ //$NON-NLS-2$
                + " - value of 'mapreduce.fileoutputcommitter.marksuccessfuljobs' -> " + conf.get("mapreduce.fileoutputcommitter.marksuccessfuljobs")); //$NON-NLS-1$ //$NON-NLS-2$
        return conf;
    }
    
}
