package com.denodo.devkit.hadoop.commons.handler.map;

import org.apache.hadoop.fs.Path;

import com.denodo.devkit.hadoop.commons.handler.IHadoopTaskHandler;
import com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.devkit.hadoop.commons.result.map.MapFileOutputFormatHadoopResultIterator;

public abstract class AbstractMapFileOutputFormatHadoopTaskHandler implements
        IHadoopTaskHandler {

    public abstract Path getOutputPath();
    
    public abstract boolean deleteOutputPathAfterReadOutput();
    
    public abstract String getDataNodeIp(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    
    public abstract String getDataNodePort(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
        
    
    @Override
    public IHadoopResultIterator getResultIterator(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters) {
        return new MapFileOutputFormatHadoopResultIterator(
                getDataNodeIp(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                getDataNodePort(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), hadoopKeyClass, hadoopValueClass,
                getOutputPath(), deleteOutputPathAfterReadOutput());        
    }
       
    
}
