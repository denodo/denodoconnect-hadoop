package com.denodo.connect.hadoop.mapreduce.wrapper.output;

import java.io.IOException;

import com.denodo.connect.hadoop.hdfs.reader.HDFSFileReader;


public class MapReduceJobAvroFileReader extends MapReduceJobFileReader {

    public MapReduceJobAvroFileReader(HDFSFileReader reader) {
        super(reader);
    }

    @Override
    public Object[] read() throws IOException {

        Object data = this.reader.read();

        return (data == null) ? null : new Object[] { data };
    }

}
