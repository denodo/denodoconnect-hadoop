package com.denodo.devkit.hadoop.commons.handler;

import com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator;


public interface IHadoopTaskHandler {

    /**
     * Returns an iterator to be able to iterate over the results. Such an iterator
     * is in charge of reading all the output files, closing them, ...
     * 
     * 
     * @return
     */
    public IHadoopResultIterator getResultIterator(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    
    /**
     * Get the parameters to be passed to the Main Class 
     * 
     * @param hostIp ip of the host to connect to
     * @param hostPort port of the host
     * @param hostUser 
     * @param hostPassword
     * @param hostTimeout
     * @param pathToJarInHost path to the jar containing the MapReduce task
     * @param mainClassInJar main class in the previous jar to be called
     * @return {@link String}[] with the parameters to be passed to mainClassInJar
     */
    public String[] getMapReduceParameters(String hostIp, String hostPort,
            String hostUser, String hostPassword, 
            String hostTimeout, String pathToJarInHost,
            String mainClassInJar, String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    
 
}
