package com.denodo.devkit.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.denodo.devkit.hadoop.WordCount.TokenCounterMapper;
import com.denodo.devkit.hadoop.WordCount.TokenCounterReducer;

public class WordCountExecuter {
	
	
	public static String OUTPUT_DIR = "output";

	/**
	 * The main entry point.
	 */
	public static void main(String[] args) throws Exception {
		
		//Establishing job configuration
	    Configuration conf = new Configuration();
	    //HDFS IP & port
	    conf.set("fs.default.name", "hdfs://172.16.0.58:8020");
	    //Jobtracker IP & Port
	    conf.set("mapred.job.tracker","172.16.0.58:8022");
	    //Remove SUCESS file from output dir
	    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
	    //Custom job configuration. This will be retrieved by the destination class 
	    conf.set("input.selectedword", "you");
	    	    
	    //Job creation 
	    Job job = new Job(conf, "Example Hadoop 0.20.2 WordCount");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenCounterMapper.class);
	    job.setReducerClass(TokenCounterReducer.class);    
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    //The file path in this case must begin with "../.." because the ssh is not connecting with the same 
	    //user that has the info we are processing
	    FileInputFormat.addInputPath(job, new Path("../../user/cloudera/input/mobyDick.txt"));
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));
	    
	    //wait for job finishing
	    boolean complete = job.waitForCompletion(true);
	    
	    
	    // If everything is ok we must read the output and delete it. Hadoop cannot stream the results so we have to wait until
	    // it finishes, read the output files and delete them
	    if (complete) {
		    FileSystem fs = FileSystem.get(conf);
		    Path outputPath = new Path(OUTPUT_DIR);
		    FileStatus[] fss = fs.listStatus(outputPath);
		    for (FileStatus status : fss) {
		        Path path = status.getPath();
		        if (!status.isDir()) {
			        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			        Text key = new Text();
			        IntWritable value = new IntWritable();
			        while (reader.next(key, value)) {
			            System.out.println(key.toString() + " | " + value.get());
			        }
			        reader.close();
		        }
		    }
		    
		    fs.delete(outputPath, true);
	    }
	    
	    //Exit with the result
	    System.exit(complete ? 0 : 1);
	}

}
