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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.denodo.connect.hadoop.hdfs.commons.naming.Parameter;
import com.denodo.connect.hadoop.hdfs.reader.keyvalue.HDFSSequenceFileReader;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobFileReader;

/**
 * Handler to work with Sequence files
 *
 */
public class SequenceFileOutputMapReduceJobHandler extends
    KeyValueFileOutputMapReduceJobHandler {


    @Override
    public MapReduceJobFileReader getOutputReader(Map<String, String> inputParameters) throws IOException {

        String hadoopKeyClass = inputParameters.get(Parameter.HADOOP_KEY_CLASS);
        String hadoopValueClass = inputParameters.get(Parameter.HADOOP_VALUE_CLASS);

        Configuration conf = getConfiguration(inputParameters);
        return new MapReduceJobFileReader(new HDFSSequenceFileReader(conf, hadoopKeyClass,
            hadoopValueClass, getOutputPath()));
    }

}
