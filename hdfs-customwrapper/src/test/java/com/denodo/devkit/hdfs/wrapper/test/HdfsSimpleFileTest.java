package com.denodo.devkit.hdfs.wrapper.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsSimpleFileTest {

    public static void main(String[] args) {
        String host = "192.168.73.132";
        int port = 8020;

        // Establishing configuration
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);
        String input_file_path = "/wordcount/output";
        String column_delimiter = "\t";
        // File to read
        Path input_path = new Path(input_file_path);
        FileSystem fileSystem;
        FSDataInputStream dataInputStream = null;
        List result = new ArrayList();
        try {
            fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(input_path)) {
                FileStatus[] fss = fileSystem.listStatus(input_path);
                for (FileStatus status : fss) {
                    Path path = status.getPath();
                    if (!status.isDir()) {
                        dataInputStream = fileSystem.open(path);
                        String line = "";
                        String[] row = new String[4];
                        while ((line = dataInputStream.readLine()) != null) {
                            String[] line_array = line.split(column_delimiter);
                            // column_delimiter matches the key/value delimiter
                            if (line_array.length == 2) {
                                row[0] = line_array[0];
                                row[1] = line_array[1];
                                row[2] = input_file_path;
                                row[3] = column_delimiter;
                                System.out.println(row[0] + " - " + row[1]
                                        + " - " + row[2] + " - " + row[3]);
                                result.add(row);
                            } else {
                                System.out
                                        .println("Column delimiter matches the key/value delimiter");
                                break;
                            }
                        }
                    }
                }
                if (dataInputStream != null)
                    dataInputStream.close();
                // try {
                // Delete path recursively after reading
                // fileSystem.delete(input_path, true);
                // System.out.println("Deleted path " + input_file_path);
                // } catch (IOException e) {
                // e.printStackTrace();
                // } finally {
                if (fileSystem != null)
                    fileSystem.close();
                // }
            } else {
                System.out.println("Path not found " + input_file_path);
                if (fileSystem != null)
                    fileSystem.close();
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
