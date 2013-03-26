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
package com.denodo.connect.hadoop.mapreduce.wrapper.commons.naming;

public final class ParameterNaming {

    // Parameters
    public static final String HOST_IP = "Host IP";
    public static final String HOST_PORT = "Host port";
    public static final String HOST_USER = "Host user";
    public static final String HOST_PASSWORD = "Host password";
    public static final String HOST_TIMEOUT = "Host timeout";

    public static final String PATH_TO_JAR_IN_HOST = "Path to jar in host";
    public static final String MAIN_CLASS_IN_JAR = "Main class in jar";

    // String with the parameters (i.e. "This is a test" 4 6 "username")
    public static final String MAPREDUCE_PARAMETERS =
        "MapReduce job parameters ";

    public static final String HADOOP_KEY = "KEY";
    public static final String HADOOP_KEY_CLASS = "Key class";
    public static final String HADOOP_VALUE = "VALUE";
    public static final String HADOOP_VALUE_CLASS = "Value class";

    public static final String IMAPREDUCETASKHANDLER_IMPLEMENTATION =
        "IMapReduceTaskHandler implementation ";

    private ParameterNaming() {

    }
}
