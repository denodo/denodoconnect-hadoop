package com.denodo.devkit.hadoop.commons.result;

import org.apache.hadoop.io.Writable;

public interface IHadoopResultIterator {

    /**
     * Reads the next key-value pair and stores it in the key and value parameters
     * 
     * @param key
     * @param value
     * @return
     */
    public <K extends Writable, V extends Writable> boolean readNext(K key, V value);
    
    /**
     * @return an instance of the key class initialized
     */
    public <K extends Writable> K getInitKey();
    
    /**
     * @return an instance of the value class initialized
     */
    public <V extends Writable> V getInitValue();
    
}
