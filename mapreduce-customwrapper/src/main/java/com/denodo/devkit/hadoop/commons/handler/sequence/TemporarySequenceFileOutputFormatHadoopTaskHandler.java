package com.denodo.devkit.hadoop.commons.handler.sequence;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;


/**
 * Handler to work with {@link SequenceFile}
 * 
 */
public class TemporarySequenceFileOutputFormatHadoopTaskHandler extends
        AbstractSequenceFileOutputFormatHadoopTaskHandler {

    private static Path outputPath;
    
    static {
        outputPath = new Path("/denodo_output_" + System.nanoTime()); //$NON-NLS-1$
    }
    
    @Override
    public String[] getMapReduceParameters(String hostIp, String hostPort,
            String hostUser, String hostPassword, String hostTimeout,
            String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters) {
        
        String[] out = mapReduceParameters.split(","); //$NON-NLS-1$
        String path = getOutputPath().toString();
        out = (String[]) ArrayUtils.add(out, path);
        return out;
    }

    @Override
    public Path getOutputPath() {
        return outputPath;
    }

    @Override
    public boolean deleteOutputPathAfterReadOutput() {
        return true;
    }


    @Override
    public String getDataNodeIp(String hostIp, String hostPort,
            String hostUser, String hostPassword, String hostTimeout,
            String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass,
            String mapReduceParameters) {
        return getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, 
                mainClassInJar, 
                hadoopKeyClass, hadoopValueClass, mapReduceParameters)[0];
    }



    @Override
    public String getDataNodePort(String hostIp, String hostPort,
            String hostUser, String hostPassword, String hostTimeout,
            String pathToJarInHost, String mainClassInJar,
            String hadoopKeyClass, String hadoopValueClass,
            String mapReduceParameters) {
        return getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword, hostTimeout, pathToJarInHost, 
                mainClassInJar, 
                hadoopKeyClass, hadoopValueClass, mapReduceParameters)[1];
    }


}
