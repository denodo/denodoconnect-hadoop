package com.denodo.connect.hdfs.wrapper.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.denodo.connect.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.connect.hadoop.commons.result.sequence.SequenceFileOutputFormatHadoopResultIterator;

public class HdfsSequecenFileTest {

    public static void main(String[] args) {
        String hostIp = "192.168.25.128";
        String hostPort = "8020";
        String inputFilePath = "/user/sandbox/sequence.seq";
        Path path = new Path(inputFilePath);
        String hadoopKeyClass = IntWritable.class.getName();
        String hadoopValueClass = Text.class.getName();
        boolean deleteOutputPathAfterReadOutput = false;

        // Process file
        IHadoopResultIterator resultIterator = new SequenceFileOutputFormatHadoopResultIterator(hostIp, hostPort, hadoopKeyClass,
                hadoopValueClass, path, deleteOutputPathAfterReadOutput);
        Writable key = resultIterator.getInitKey();
        Writable value = resultIterator.getInitValue();
        while (resultIterator.readNext(key, value)) {
            System.out.println("KEY: " + key + " | VALUE: " + value);
        }
    }
}
