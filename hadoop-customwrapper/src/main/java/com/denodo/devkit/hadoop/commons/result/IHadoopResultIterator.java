package com.denodo.devkit.hadoop.commons.result;

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
