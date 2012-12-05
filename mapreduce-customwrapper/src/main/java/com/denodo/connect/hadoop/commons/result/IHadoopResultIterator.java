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
package com.denodo.connect.hadoop.commons.result;

import org.apache.hadoop.io.Writable;

public interface IHadoopResultIterator {

    /**
     * Reads the next key-value pair and stores it in the key and value parameters
     * This method is in charge of reading every output file and closing them in a 
     * transparent way
     * 
     * @param key
     * @param value
     * @return
     */
    public <K extends Writable, V extends Writable> boolean readNext(K key, V value);
    
    /**
     * @return an instance of the key class initialized (necessary
     * to read output)
     */
    public <K extends Writable> K getInitKey();
    
    /**
     * @return an instance of the value class initialized (necessary
     * to read output)
     */
    public <V extends Writable> V getInitValue();
    
}
