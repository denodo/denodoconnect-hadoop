/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2013, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.hdfs.util.configuration;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public final class InitUtils {


    private InitUtils() {

    }

    /**
     * It returns a Writable initialized necessary to read mapreduce output.
     * Key class can't be an array.
     *
     * @param hadoopKeyClass class of the Writable
     * @return the Writable of class hadoopKeyClass initialized
     */
    public static Writable getInitKey(String hadoopKeyClass, Configuration configuration) {
        try {
            return (Writable) ReflectionUtils.newInstance(Class.forName(hadoopKeyClass), configuration);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class name " + hadoopKeyClass
                + " not found", e);
        }
    }


    /**
     * It returns a Writable initialized necessary to read mapreduce output.
     * If hadoopValuClass ends with [] it will return an ArrayWritable of the
     * class in hadoopValueClass.
     */
    @SuppressWarnings("unchecked")
    public static Writable getInitValue(String hadoopValueClass, Configuration configuration) {
        try {
            if (StringUtils.endsWith(hadoopValueClass, "[]")) {
                return new ArrayWritable((Class<? extends Writable>) Class.forName(StringUtils.substringBeforeLast(hadoopValueClass, "[]")));
            }
            return (Writable) ReflectionUtils.newInstance(Class.forName(hadoopValueClass), configuration);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class name " + hadoopValueClass
                + " not found", e);
        }
    }


}
