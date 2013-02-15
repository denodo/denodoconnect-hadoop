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
package com.denodo.connect.hadoop.commons.naming;

public class ParameterNaming {

    // Parameters
    public static String HOST_IP = "Host IP"; //$NON-NLS-1$
    public static String HOST_PORT = "Host port"; //$NON-NLS-1$
    public static String HOST_USER = "Host user"; //$NON-NLS-1$
    public static String HOST_PASSWORD = "Host password"; //$NON-NLS-1$
    public static String HOST_TIMEOUT = "Host timeout"; //$NON-NLS-1$

    public static String PATH_TO_JAR_IN_HOST = "Path to jar in host"; //$NON-NLS-1$
    public static String MAIN_CLASS_IN_JAR = "Main class in jar"; //$NON-NLS-1$

    // String with the parameters (i.e. "This is a test" 4 6 "username")
    public static String MAPREDUCE_PARAMETERS = "MapReduce parameters (it will be added to the call to the MapReduce job)"; //$NON-NLS-1$

    public static String HADOOP_KEY = "KEY"; //$NON-NLS-1$
    public static String HADOOP_KEY_CLASS = "KEY_CLASS"; // Hadoop class //$NON-NLS-1$
    public static String HADOOP_VALUE = "VALUE"; //$NON-NLS-1$
    public static String HADOOP_VALUE_CLASS = "VALUE_CLASS"; // Hadoop class //$NON-NLS-1$

    public static String CLASS_IMPLEMENTING_IHADOOPTASKHANDLER = "CLASS_IMPLEMENTING_IHADOOPTASKHANDLER"; //$NON-NLS-1$

    public static String DELETE_AFTER_READING = "Delete after reading";//$NON-NLS-1$
    public static final String SEPARATOR = "separator";//$NON-NLS-1$
    public static String INPUT_FILE_PATH = "inputFilePath";//$NON-NLS-1$

}
