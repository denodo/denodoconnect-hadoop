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
package com.denodo.connect.hadoop.mapreduce.wrapper.handler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.util.configuration.HadoopConfigurationUtils;

/**
 * Abstract handler to work with file outputs
 *
 */
public abstract class AbstractFileOutputMapReduceJobHandler
    implements MapReduceJobHandler {

    private Path outputPath = new Path("denodo_output_" + System.nanoTime());

    @Override
    public Collection<String> getJobParameters(Map<String, String> inputValues) {

        String path = getOutputPath().toString();

        return Arrays.asList(path);
    }

    /**
     * @return path where the job output will be written to
     */
    protected Path getOutputPath() {
        return this.outputPath;
    }

    protected static Configuration getConfiguration(Map<String, String> inputParameters) {

        String fileSystemURI = inputParameters.get(Parameter.FILESYSTEM_URI);
        return HadoopConfigurationUtils.getConfiguration(fileSystemURI);
    }

    /**
     * Validates input parameters given by the user.
     */
    public abstract void checkInput(Map<String, String> inputValues);
}
