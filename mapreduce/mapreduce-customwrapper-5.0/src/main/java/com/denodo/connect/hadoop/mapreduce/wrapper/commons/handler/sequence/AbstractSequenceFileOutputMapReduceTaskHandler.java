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

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.mapreduce.wrapper.commons.handler.IMapReduceTaskHandler;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.output.IMapReduceTaskOutputReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.commons.output.sequence.SequenceFileReader;

/**
 * Abstract handler to work with {@link SequenceFile} It implements the
 * getOutputReader method
 *
 */
public abstract class AbstractSequenceFileOutputMapReduceTaskHandler
    implements IMapReduceTaskHandler {

    /**
     * @return path where the task output will be written to
     */
    public abstract Path getOutputPath();

    /**
     * @return whether to delete outputPath once all the results are read or not
     */
    public abstract boolean deleteOutputPathAfterReadOutput();

    /**
     * It returns the dataNodeIp. It receives all the input parameters of the
     * base view
     */
    public abstract String getDataNodeIp(String hostIp, String hostPort,
        String hostUser, String hostPassword, String hostTimeout,
        String pathToJarInHost, String mainClassInJar, String hadoopKeyClass,
        String hadoopValueClass, String mapReduceParameters);

    /**
     * It returns the dataNodePort. It receives all the input parameters of the
     * base view
     */
    public abstract String getDataNodePort(String hostIp, String hostPort,
        String hostUser, String hostPassword, String hostTimeout,
        String pathToJarInHost, String mainClassInJar, String hadoopKeyClass,
        String hadoopValueClass, String mapReduceParameters);

    @Override
    public IMapReduceTaskOutputReader getOutputReader(String hostIp,
        String hostPort, String hostUser, String hostPassword,
        String hostTimeout, String pathToJarInHost, String mainClassInJar,
        String hadoopKeyClass, String hadoopValueClass,
        String mapReduceParameters) throws IOException {

        String dataNodeIP = getDataNodeIp(
            hostIp, hostPort, hostUser, hostPassword, hostTimeout,
            pathToJarInHost, mainClassInJar, hadoopKeyClass, hadoopValueClass,
            mapReduceParameters);

        String dataNodePort = getDataNodePort(hostIp, hostPort, hostUser,
            hostPassword, hostTimeout, pathToJarInHost, mainClassInJar,
            hadoopKeyClass, hadoopValueClass, mapReduceParameters);

        return new SequenceFileReader(dataNodeIP, dataNodePort, hadoopKeyClass,
            hadoopValueClass, getOutputPath(), deleteOutputPathAfterReadOutput());
    }

}
