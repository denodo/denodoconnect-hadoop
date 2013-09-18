package com.denodo.connect.hadoop.mapreduce.wrapper.output;

import java.io.IOException;


public interface MapReduceJobOutputReader {

    public Object[] read() throws IOException;

    public void close() throws IOException;

}
