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

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class TypeUtils {
    
    private static final  Logger LOG = LoggerFactory.getLogger(TypeUtils.class);    


    private TypeUtils() {

    }

    public static Class<?> toJava(final String hadoopClass) {

        if (NullWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Void.class;
        }
        if (Text.class.getName().equalsIgnoreCase(hadoopClass)) {
            return String.class;
        }
        if (ShortWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Short.class;
        }
        if (IntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.class;
        }
        if (VIntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.class;
        }        
        if (LongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Long.class;
        }
        if (VLongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
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
            return Byte.class;
        }
        if (BytesWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return byte[].class;
        }        
        if (StringUtils.endsWith(hadoopClass, "[]")) {
            return List.class;
        }

        LOG.warn("Class '" + hadoopClass + "' is not supported. Returning String.class");
        return String.class;
    }

    public static int toSQL(final Class<?> javaClass) {

        if (Void.class.equals(javaClass)) {
            return Types.NULL;
        }
        if (String.class.equals(javaClass)) {
            return Types.VARCHAR;
        }
        if (Short.class.equals(javaClass)) {
            return Types.SMALLINT;
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
        if (Byte.class.equals(javaClass)) {
            return Types.BIT;
        }        
        if (byte[].class.equals(javaClass)) {
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
        if (BigDecimal.class.equals(javaClass)) {
            return Types.DECIMAL;
        }
        if (java.util.Date.class.equals(javaClass)) {
            return Types.DATE;
        }

        LOG.warn("Class '" + javaClass + "' is not supported. Returning Types.VARCHAR");
        return Types.VARCHAR;
    }

    /**
     * Converts the given {@link Writable} value to a Java object valid
     * (String, Long, Int, Array). In case it is an ArrayWritable, it will
     * return an Object[] with the values converted (based on the value of
     * hadoopClass).
     */
    public static Object getValue(final String hadoopClass, final Writable value) {

        if (value instanceof NullWritable) {
            return null;
        }
        if (Text.class.getName().equalsIgnoreCase(hadoopClass)) {
            return ((Text) value).toString();
        }
        if (ShortWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Short.valueOf(((ShortWritable) value).get());
        }        
        if (IntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.valueOf(((IntWritable) value).get());
        }
        if (VIntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Integer.valueOf(((IntWritable) value).get());
        }        
        if (LongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Long.valueOf(((LongWritable) value).get());
        }
        if (VLongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
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
        if (BytesWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return ((BytesWritable) value).getBytes();
        }
        if (ByteWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Byte.valueOf(((ByteWritable) value).get());
        }        
        // If it ends with [] -> It's an array
        if (StringUtils.endsWith(hadoopClass, "[]")) {
            final ArrayWritable aw = (ArrayWritable) value;
            final List<Object> data = new ArrayList<Object>();
            for (final Writable item : aw.get()) {
                data.add(getValue(StringUtils.substringBeforeLast(hadoopClass, "[]"), item));
            }
            return data.toArray(new Object[data.size()]);
        }

        LOG.warn("Class '" + hadoopClass + "' is not supported. Returning its writable.toString() value");
        return value.toString();
    }

    /**
     *
     * Default Hadoop class is org.apache.hadoop.io.Text
     */
    public static String getHadoopClass(final String hadoopClass) {
        return (hadoopClass != null) ? hadoopClass : Text.class.getName();
    }


}
