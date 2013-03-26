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
package com.denodo.connect.hadoop.mapreduce.wrapper.commons.output;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface IMapReduceTaskOutputReader {

    /**
     * Reads the next key-value pair and stores it in the key and value
     * parameter.
     */
    public boolean readNext(Writable key, Writable value) throws IOException;

    /**
     * @return an instance of the key class initialized (necessary to read
     *         output)
     */
    public Writable getInitKey();

    /**
     * @return an instance of the value class initialized (necessary to read
     *         output)
     */
    public Writable getInitValue();

    /**
     * Closes the reader and releases any resources associated with it.
     */
    public void close() throws IOException;

}
