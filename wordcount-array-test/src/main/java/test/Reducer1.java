package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer1 extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, ArrayWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, ArrayWritable> output, Reporter reporter)
			throws IOException {
		 
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		 
		System.out.println("[REDUCER] Output: " + key + " - " + sum);
		
		ArrayWritable aw = new ArrayWritable(Text.class);
//		aw.set(new Text[]{new Text(String.valueOf(sum)), new Text("now it works"), new Text("[6,7,7,45]"), new Text("546546"),
//		new Text("now it works 7"), new Text("now it works 9")});
		  
		aw.set(new Text[]{new Text(String.valueOf(sum)), new Text("now it works")});
		
		System.out.println (aw.toStrings());
        System.out.println (StringUtils.join(aw.toStrings(), "--"));
        
		output.collect(key, 
		        aw);
		
	}

	


	

}
