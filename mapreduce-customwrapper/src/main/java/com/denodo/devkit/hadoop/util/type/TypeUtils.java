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
package com.denodo.devkit.hadoop.util.type;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.InternalErrorException;

public class TypeUtils {

    private static final Logger logger = Logger.getLogger(TypeUtils.class);

    /**
     * @param hadoopClass hadoop class (package + class name). In case it is an 
     * {@link ArrayWritable}({@link LongWritable}) it would be {@link LongWritable}[]
     * 
     * @throws UnsupportedOperationException in case the value of hadoopClass
     * is not supported
     * 
     * @return the {@link Types} value for the given hadoopClass
     */
    public static int getSqlType(String hadoopClass) {
        if (Text.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.VARCHAR;
        }
        if (IntWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.INTEGER;
        }
        if (LongWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.BIGINT;
        }
        if (BooleanWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.BOOLEAN;
        }
        if (DoubleWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.DOUBLE;
        }
        if (FloatWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.FLOAT;
        }
        // If it ends with [] -> It's an array
        if (StringUtils.endsWith(hadoopClass, "[]")) { //$NON-NLS-1$
            return Types.ARRAY;
        }
        
        logger.warn("Class '" + hadoopClass + "' is not directly supported. Returning Types.VARCHAR"); //$NON-NLS-1$ //$NON-NLS-2$
        return Types.VARCHAR;
    }
    
    /**
     * It converts the given {@link Writable} value to a java object valid for VDP
     * (String, Long, Int, Array). In case it is an ArrayWritable, it will
     * return an Object[] with the values converted (based on the value of
     * hadoopClass)
     * 
     * @param hadoopClass
     * @param value
     * 
     * @throws UnsupportedOperationException in case the value of hadoopClass
     * is not supported
     * 
     * @return
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
        if (StringUtils.endsWith(hadoopClass, "[]")) { //$NON-NLS-1$
            ArrayWritable aw = (ArrayWritable) value;
            List<Object> data = new ArrayList<Object>();
            for (Writable item : aw.get()) {
                data.add(getValue(StringUtils.substringBeforeLast(hadoopClass, "[]"), item)); //$NON-NLS-1$
            }
            return data.toArray(new Object[data.size()]);
        }
                
        logger.warn("Class '" + hadoopClass + "' is not directly supported. Returning its writable.toString() value"); //$NON-NLS-1$ //$NON-NLS-2$
        return value.toString();        
    }
    
    
    /**
     * It returns a Writable initialized necessary to read mapreduce output
     * Key class can't be an array
     * 
     * @param hadoopKeyClass class of the Writable
     * @param configuration
     * @return the Writable of class hadoopKeyClass initialized
     */
    public static <K extends Writable> K getInitKey(String hadoopKeyClass, Configuration configuration) {
        try { 
            @SuppressWarnings("unchecked")
            K key = (K) ReflectionUtils.newInstance(Class.forName(hadoopKeyClass), configuration);
            return key;
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException("There has been an error initializing key" + hadoopKeyClass, e); //$NON-NLS-1$
        }
    }

    
    /**
     * It returns a Writable initialized necessary to read mapreduce output
     * If hadoopValuClass ends with [] it will return an ArrayWritable of the 
     * class in hadoopValueClass
     * 
     * @param hadoopValueClass class of the Writable
     * @param configuration
     * @return
     */
    public static <V extends Writable> V getInitValue(String hadoopValueClass, Configuration configuration) {
        try {
            if (StringUtils.endsWith(hadoopValueClass, "[]")) { //$NON-NLS-1$
                @SuppressWarnings("unchecked")
                V value = (V) new ArrayWritable((Class<Writable>) Class.forName(StringUtils.substringBeforeLast(hadoopValueClass, "[]"))); //$NON-NLS-1$
                return value;
            }
            @SuppressWarnings("unchecked")
            V value = (V) ReflectionUtils.newInstance(Class.forName(hadoopValueClass), configuration);
            return value;
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException("There has been an error initializing value" + hadoopValueClass, e); //$NON-NLS-1$
        }
    }

    
}
