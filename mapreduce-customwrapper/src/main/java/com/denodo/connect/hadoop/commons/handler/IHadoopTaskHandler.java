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
package com.denodo.connect.hadoop.commons.handler;

import com.denodo.connect.hadoop.commons.result.IHadoopResultIterator;


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
            String mainClassInJar, 
            String hadoopKeyClass, String hadoopValueClass, String mapReduceParameters);
    
 
}
