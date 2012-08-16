package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer1 extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		
		System.out.println("[REDUCER] Output: " + key + " - " + sum);
		
		output.collect(key, new IntWritable(sum));
		
	}

	

	

}
