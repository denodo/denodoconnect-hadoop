package com.denodo.connect.hadoop.mapreduce.wrapper.output;

import java.io.IOException;

import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;


public class MapReduceJobFileReader implements MapReduceJobOutputReader {

    protected HDFSFileReader reader;

    public MapReduceJobFileReader(HDFSFileReader reader) {
        this.reader = reader;
    }

    @Override
    public Object[] read() throws IOException {
        return (Object[]) this.reader.read();
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
        this.reader.delete();
    }
}
