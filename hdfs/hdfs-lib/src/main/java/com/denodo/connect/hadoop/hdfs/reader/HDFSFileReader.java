package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;


public interface HDFSFileReader {


    public Object read() throws IOException;

    public void close() throws IOException;

    public void delete() throws IOException;
}
