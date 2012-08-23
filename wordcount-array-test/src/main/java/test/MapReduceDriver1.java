package test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class MapReduceDriver1 {

	public static void main(String[] args) {
		int exitCode = -1;
	    
	    System.out.println("Starting MapReduceDriver1");
		
		
		String datanodeIp = args[0];
		String datanodePort = args[1];
		String jobtrackerIp = args[2];
		String jobtrackerPort = args[3];
		String input = args[4];
		String output = args[5];
		
		System.out.println("datanodeIp: " + datanodeIp);
		System.out.println("datanodePort: " + datanodePort);
		System.out.println("jobtrackerIp: " + jobtrackerIp);
		System.out.println("jobtrackerPort: " + jobtrackerPort);
		System.out.println("input: " + input);
		System.out.println("output: " + output);
		
		
		JobConf conf = new JobConf(test.MapReduceDriver1.class);
		conf.setJobName("abc-" + System.nanoTime());

		//fs.default.name
		conf.set("defaultFS", "hdfs://" + datanodeIp + ":" + datanodePort);
		conf.set("mapreduce.jobtracker.address", jobtrackerIp + ":" + jobtrackerPort);
		//Remove SUCCESS file from output dir
	    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
		  
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(ArrayWritable.class);
		 
		conf.setMapperClass(test.Mapper1.class);
		conf.setReducerClass(test.Reducer1.class);

		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		// Input and output are in hdfs
		FileInputFormat.setInputPaths(conf, new Path(input));
	    FileOutputFormat.setOutputPath(conf, new Path(output));

	    
	    
	    System.out.println("Run job");
	    try {

	        RunningJob rj = JobClient.runJob(conf);

	        System.out.println("Job id: " + rj.getID().getId());


	    } catch (Exception e) {
	        System.out.println("Errror: " + e.getMessage());
	        e.printStackTrace();
	    }
    
	    
	    
		System.out.println("Ending MapReduceDriver1");
		
	}

}
