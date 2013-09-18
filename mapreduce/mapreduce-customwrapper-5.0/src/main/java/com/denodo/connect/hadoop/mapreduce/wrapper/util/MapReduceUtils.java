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
package com.denodo.connect.hadoop.mapreduce.wrapper.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;

public final class MapReduceUtils {


    private MapReduceUtils() {

    }

    public static String buildFileSystemURI(String dataNodeIP, String dataNodePort) {
        return "hdfs://" + dataNodeIP + ":" + dataNodePort;
    }

    /**
     * It returns the command to be executed:
     * hadoop jar PATH_TO_JAR_IN_HOST MAIN_CLASS_IN_JAR DATANODE_IP DATANODE_PORT
     * JOBTRACKER_IP JOBTRACKER_PORT INPUT_FILE_PATH OUTPUT_FILE_PATH
     *
     * (i.e. hadoop jar /home/cloudera/jars/hadooptestwordcount-1.0-SNAPSHOT.jar
     * test.MapReduceDriver1 172.16.0.58 8020 172.16.0.58 8021
     * /user/cloudera/input /user/cloudera/output
     *
     * The specific parameters to be added to
     * "hadoop jar PATH_TO_JAR_IN_HOST MAIN_CLASS_IN_JAR MAPREDUCE_PARAMETERS" come from
     * {@link MapReduceJobHandler#getJobParameters()}
     */
    public static String getCommand(String jar, String mainClass, String jobParams,
        Collection<String> jobSpecificParams) {

        Collection<String> command = new ArrayList<String>();
        command.add("hadoop");
        command.add("jar");
        command.add(jar);
        command.add(mainClass);

        if (jobParams != null) {
            String[] out = jobParams.split(",");
            command.addAll(Arrays.asList(out));
        }

        command.addAll(jobSpecificParams);

        return StringUtils.join(command.toArray(), " ");
    }

}
