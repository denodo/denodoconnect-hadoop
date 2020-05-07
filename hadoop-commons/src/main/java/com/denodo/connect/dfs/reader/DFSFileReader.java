package com.denodo.connect.dfs.reader;

import java.io.IOException;


public interface DFSFileReader {


    public Object read() throws IOException;

    public void close() throws IOException;

    public void delete() throws IOException;
}
