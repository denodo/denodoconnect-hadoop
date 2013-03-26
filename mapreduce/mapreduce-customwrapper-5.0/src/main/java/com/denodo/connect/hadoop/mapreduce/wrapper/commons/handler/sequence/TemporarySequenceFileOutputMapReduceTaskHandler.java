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
package com.denodo.connect.hadoop.mapreduce.wrapper.commons.handler.sequence;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;

/**
 * Handler to work with {@link SequenceFile}
 *
 */
public class TemporarySequenceFileOutputMapReduceTaskHandler extends
    AbstractSequenceFileOutputMapReduceTaskHandler {

    private static Path outputPath;

    static {
        outputPath = new Path("/denodo_output_" + System.nanoTime());
    }

    @Override
    public String[] getMapReduceParameters(String hostIp, String hostPort,
        String hostUser, String hostPassword, String hostTimeout,
        String pathToJarInHost, String mainClassInJar, String hadoopKeyClass,
        String hadoopValueClass, String mapReduceParameters) {

        String[] out = mapReduceParameters.split(",");
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
        String pathToJarInHost, String mainClassInJar, String hadoopKeyClass,
        String hadoopValueClass, String mapReduceParameters) {

        return getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword,
            hostTimeout, pathToJarInHost, mainClassInJar, hadoopKeyClass,
            hadoopValueClass, mapReduceParameters)[0];
    }

    @Override
    public String getDataNodePort(String hostIp, String hostPort,
        String hostUser, String hostPassword, String hostTimeout,
        String pathToJarInHost, String mainClassInJar, String hadoopKeyClass,
        String hadoopValueClass, String mapReduceParameters) {

        return getMapReduceParameters(hostIp, hostPort, hostUser, hostPassword,
            hostTimeout, pathToJarInHost, mainClassInJar, hadoopKeyClass,
            hadoopValueClass, mapReduceParameters)[1];
    }

}
