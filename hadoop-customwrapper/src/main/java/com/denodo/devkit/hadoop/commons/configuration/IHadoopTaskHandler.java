package com.denodo.devkit.hadoop.commons.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public interface IHadoopTaskHandler {

//    public Path getOutputPath();
    
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
    
    /**
     * @param hostIp
     * @param hostPort
     * @param hostUser
     * @param hostPassword
     * @param hostTimeout
     * @param pathToJarInHost
     * @param mainClassInJar
     * @return a {@link Map} representing the data in the output folder 
     */
    public <X extends Writable, Y extends Writable> LinkedHashMap<X, Y> readOutput(String hostIp, String hostPort,
            String hostUser, String hostPassword, 
            String hostTimeout, String pathToJarInHost,
            String mainClassInJar, String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    //TODO To avoid clone, it should return object, object using hadoopkeyclass and hadoopvalueclass (not generics to avoid user having to 
    //provide class implementing this)
}
