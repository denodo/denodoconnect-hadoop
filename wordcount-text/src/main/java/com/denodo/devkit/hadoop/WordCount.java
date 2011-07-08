package com.denodo.devkit.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
/**
 * The map class of WordCount.
 */
public static class TokenCounterMapper
    extends Mapper<Object, Text, Text, IntWritable> {
        
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            this.word.set(itr.nextToken());
            
            //Getting selected word from job configuration
            String selectedword = context.getConfiguration().get("input.selectedword");
            
            if (selectedword == null) {
            	context.write(this.word, one);
            }else if (this.word.toString().equals(selectedword)) {
            	context.write(this.word, one);
            }
        }
    }
}
/**
 * The reducer class of WordCount
 */
public static class TokenCounterReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

}  
