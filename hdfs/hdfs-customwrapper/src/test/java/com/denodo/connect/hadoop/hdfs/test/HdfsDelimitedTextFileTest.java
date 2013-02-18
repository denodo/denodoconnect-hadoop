package com.denodo.connect.hadoop.hdfs.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.denodo.connect.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.connect.hadoop.commons.result.text.TextFileOutputFormatHadoopResultIterator;

/**
 * Class to test the delimited text file reader
 * 
 */
public class HdfsDelimitedTextFileTest {

    public static void main(String[] args) {
        String hostIp = "192.168.25.128";
        String hostPort = "8020";
        String inputFilePath = "/user/sandbox/text";
        // String inputFilePath = "/user/sandbox/wordcount/wordcount_output";
        // String inputFilePath =
        // "/user/sandbox/wordcount/wordcount_output_empty";
        // String inputFilePath =
        // "/user/sandbox/wordcount/wordcount_output_nonexistent";

        String separator = "\t";
        Path path = new Path(inputFilePath);
        boolean deleteOutputPathAfterReadOutput = false;

        // Process file
        IHadoopResultIterator resultIterator = new TextFileOutputFormatHadoopResultIterator(hostIp, hostPort, separator, path,
                deleteOutputPathAfterReadOutput);
        Writable key = resultIterator.getInitKey();
        Writable value = resultIterator.getInitValue();
        while (resultIterator.readNext(key, value)) {
            System.out.println("KEY: " + key + " | VALUE: " + value);
        }
    }
}
