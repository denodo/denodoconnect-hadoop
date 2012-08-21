package com.denodo.devkit.hadoop.commons.configuration;

import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.denodo.devkit.hadoop.util.HadoopReadOutputUtils;
import com.denodo.devkit.hadoop.util.HadoopUtils;

public abstract class AbstractSequenceFileOutputFormatHadoopTaskHandler implements
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
    public LinkedHashMap<Writable, Writable> readOutput(String hostIp,
            String hostPort, String hostUser, String hostPassword,
            String hostTimeout, String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters) {
                
        LinkedHashMap<Writable, Writable> output = HadoopReadOutputUtils
                .getRowsFromSequenceFileOutputFormat(getOutputPath(), 
                        getDataNodeIp(hostIp, hostPort, hostUser, hostPassword, hostTimeout, 
                                pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                        getDataNodePort(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, 
                                mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                        hadoopKeyClass, hadoopValueClass);
        
        if (deleteOutputPathAfterReadOutput()) {
            HadoopUtils.deleteFile(getDataNodeIp(hostIp, hostPort, hostUser, hostPassword, hostTimeout, 
                    pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
            getDataNodePort(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, 
                    mainClassInJar, hadoopKeyClass, hadoopValueClass, mapReduceParameters), 
                    hadoopKeyClass, hadoopValueClass, getOutputPath());
        }
        
        return output;
    }
}
