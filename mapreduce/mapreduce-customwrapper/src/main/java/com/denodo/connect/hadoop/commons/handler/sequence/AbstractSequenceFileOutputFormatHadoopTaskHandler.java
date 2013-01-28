/*
 * =============================================================================
 * 
 *   This software is part of the DenodoConnect component collection.
 *   
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 * =============================================================================
 */
package com.denodo.connect.hadoop.commons.handler.sequence;

import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.commons.handler.IHadoopTaskHandler;
import com.denodo.connect.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.connect.hadoop.commons.result.sequence.SequenceFileOutputFormatHadoopResultIterator;

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
     * @see com.denodo.connect.hadoop.commons.handler.IHadoopTaskHandler#getResultIterator(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
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
