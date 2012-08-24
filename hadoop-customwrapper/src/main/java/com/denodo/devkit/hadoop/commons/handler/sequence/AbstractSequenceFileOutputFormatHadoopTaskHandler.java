package com.denodo.devkit.hadoop.commons.handler.sequence;

import org.apache.hadoop.fs.Path;

import com.denodo.devkit.hadoop.commons.handler.IHadoopTaskHandler;
import com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.devkit.hadoop.commons.result.sequence.SequenceFileOutputFormatHadoopResultIterator;

/**
 * Abstract handler to work with {@link SequenceFile}
 * It implements the getResultIterator method
 *
 */
public abstract class AbstractSequenceFileOutputFormatHadoopTaskHandler implements
        IHadoopTaskHandler {

    /**
     * @return path where the task output will be written to
     */
    public abstract Path getOutputPath();
    
    /**
     * @return whether to delete outputPath once all the results are read or not
     */
    public abstract boolean deleteOutputPathAfterReadOutput();
    
    /**
     * It returns the dataNodeIp. It receives all the input parameters of the base view
     * 
     * @param hostIp
     * @param hostPort
     * @param hostUser
     * @param hostPassword
     * @param hostTimeout
     * @param pathToJarInHost
     * @param mainClassInJar
     * @param hadoopKeyClass
     * @param hadoopValueClass
     * @param mapReduceParameters
     * @return
     */
    public abstract String getDataNodeIp(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    
    /**
     * It returns the dataNodePort. It receives all the input parameters of the base view
     * 
     * @param hostIp
     * @param hostPort
     * @param hostUser
     * @param hostPassword
     * @param hostTimeout
     * @param pathToJarInHost
     * @param mainClassInJar
     * @param hadoopKeyClass
     * @param hadoopValueClass
     * @param mapReduceParameters
     * @return
     */
    public abstract String getDataNodePort(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
        
    
    /* (non-Javadoc)
     * @see com.denodo.devkit.hadoop.commons.handler.IHadoopTaskHandler#getResultIterator(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public IHadoopResultIterator getResultIterator(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters) {
        return new SequenceFileOutputFormatHadoopResultIterator(
                getDataNodeIp(hostIp, hostPort, hostUser, hostPassword, hostTimeout, 
                        pathToJarInHost, mainClassInJar, 
                        hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                getDataNodePort(hostIp, hostPort, hostUser, hostPassword, hostTimeout, 
                        pathToJarInHost, mainClassInJar,  
                        hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                hadoopKeyClass, hadoopValueClass,
                getOutputPath(), deleteOutputPathAfterReadOutput());        
    }
       
    
}
