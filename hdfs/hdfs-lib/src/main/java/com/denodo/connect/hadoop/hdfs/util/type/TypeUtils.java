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
package com.denodo.connect.hadoop.hdfs.util.type;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public final class TypeUtils {

    private static final Logger logger = Logger.getLogger(TypeUtils.class);

    private TypeUtils() {

    }

    public static Class<?> toJava(String hadoopClass) {

        if (NullWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Void.class;
        }
        if (Text.class.getName().equalsIgnoreCase(hadoopClass)) {
            return String.class;
        }
        if (IntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.class;
        }
        if (LongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Long.class;
        }
        if (BooleanWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Boolean.class;
        }
        if (DoubleWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Double.class;
        }
        if (FloatWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Float.class;
        }
        if (ByteWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return ByteBuffer.class;
        }
        if (StringUtils.endsWith(hadoopClass, "[]")) {
            return List.class;
        }

        logger.warn("Class '" + hadoopClass + "' is not supported. Returning String.class");
        return String.class;
    }

    public static int toSQL(Class<?> javaClass) {

        if (Void.class.equals(javaClass)) {
            return Types.NULL;
        }
        if (String.class.equals(javaClass)) {
            return Types.VARCHAR;
        }
        if (Integer.class.equals(javaClass)) {
            return Types.INTEGER;
        }
        if (Long.class.equals(javaClass)) {
            return Types.BIGINT;
        }
        if (Boolean.class.equals(javaClass)) {
            return Types.BOOLEAN;
        }
        if (Double.class.equals(javaClass)) {
            return Types.DOUBLE;
        }
        if (Float.class.equals(javaClass)) {
            return Types.FLOAT;
        }
        if (ByteBuffer.class.equals(javaClass)) {
            return Types.VARBINARY;
        }
        if (List.class.equals(javaClass)) {
            return Types.ARRAY;
        }
        if (Map.class.equals(javaClass)) {
            return Types.ARRAY;
        }
        if (Object.class.equals(javaClass)) {
            return Types.STRUCT;
        }

        logger.warn("Class '" + javaClass + "' is not supported. Returning Types.VARCHAR");
        return Types.VARCHAR;
    }

    /**
     * Converts the given {@link Writable} value to a Java object valid
     * (String, Long, Int, Array). In case it is an ArrayWritable, it will
     * return an Object[] with the values converted (based on the value of
     * hadoopClass).
     */
    public static Object getValue(String hadoopClass, Writable value) {

        if (value instanceof NullWritable) {
            return null;
        }
        if (Text.class.getName().equalsIgnoreCase(hadoopClass)) {
            return ((Text) value).toString();
        }
        if (IntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.valueOf(((IntWritable) value).get());
        }
        if (LongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Long.valueOf(((LongWritable) value).get());
        }
        if (BooleanWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Boolean.valueOf(((BooleanWritable) value).get());
        }
        if (DoubleWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Double.valueOf(((DoubleWritable) value).get());
        }
        if (FloatWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Float.valueOf(((FloatWritable) value).get());
        }
        // If it ends with [] -> It's an array
        if (StringUtils.endsWith(hadoopClass, "[]")) {
            ArrayWritable aw = (ArrayWritable) value;
            List<Object> data = new ArrayList<Object>();
            for (Writable item : aw.get()) {
                data.add(getValue(StringUtils.substringBeforeLast(hadoopClass, "[]"), item));
            }
            return data.toArray(new Object[data.size()]);
        }

        logger.warn("Class '" + hadoopClass + "' is not supported. Returning its writable.toString() value");
        return value.toString();
    }

    /**
     *
     * Default Hadoop class is org.apache.hadoop.io.Text
     */
    public static String getHadoopClass(String hadoopClass) {
        return (hadoopClass != null) ? hadoopClass : Text.class.getName();
    }


}
