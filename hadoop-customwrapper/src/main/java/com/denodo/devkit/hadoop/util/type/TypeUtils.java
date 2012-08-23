package com.denodo.devkit.hadoop.util.type;

import java.sql.Types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class TypeUtils {

    //TODO ArrayWritabl is of Text
    
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(TypeUtils.class);

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
        if (ArrayWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return Types.ARRAY;
        }
        
        throw new UnsupportedOperationException("Type '" + hadoopClass + "' is not supported"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
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
        if (ArrayWritable.class.getName().equalsIgnoreCase(hadoopClass)) {
            return (((ArrayWritable) value).toStrings());
        }  
        
        //TODO Should return tostring
        throw new UnsupportedOperationException("Type not supported " + hadoopClass); //$NON-NLS-1$
    }
    
}
