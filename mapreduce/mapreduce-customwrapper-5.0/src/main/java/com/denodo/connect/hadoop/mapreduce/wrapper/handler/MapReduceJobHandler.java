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
import java.util.Collection;
import java.util.Map;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.mapreduce.wrapper.output.MapReduceJobOutputReader;

public interface MapReduceJobHandler {


    /**
     * Get the specific parameters to be passed to the Main Class.
     */
    public Collection<String> getJobParameters(Map<String, String> inputParameters);

    /**
     * Defines the schema of the data that produces the job at its output.
     */
    public Collection<SchemaElement> getSchema(Map<String, String> inputParameters);

    /**
     * Returns a reader to be able to iterate over the results. Such a
     * reader is in charge of reading all the output files and closing them.
     */
    public MapReduceJobOutputReader getOutputReader(Map<String, String> inputParameters) throws IOException;

}
