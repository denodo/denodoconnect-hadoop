package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;

import org.apache.hadoop.io.Writable;


public interface HDFSKeyValueReader {

    /**
     * Reads the next key-value pair and stores it in the key and value
     * parameters This method is in charge of reading every output file and
     * closing them in a transparent way
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

    public void close() throws IOException;
}
