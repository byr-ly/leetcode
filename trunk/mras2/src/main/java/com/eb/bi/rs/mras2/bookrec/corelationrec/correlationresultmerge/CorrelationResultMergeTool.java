package com.eb.bi.rs.mras2.bookrec.corelationrec.correlationresultmerge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;



public class CorrelationResultMergeTool extends BaseDriver {
	
	public static void rmr(String folder, Configuration conf)
			throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(conf);
		fs.deleteOnExit(path);
		fs.close();
	}
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration(getConf());
		Job job = new Job(conf);
		
		job.setJarByClass(CorrelationResultMergeTool.class);
		job.setJobName("CorrelationResultMerge");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(ResultMergeReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "10"));
		job.setNumReduceTasks(reduceNum);
		
		
		String readPath = properties.getProperty("conf.input.read.path");
		String viewPath = properties.getProperty("conf.input.view.path");
		String orderPath = properties.getProperty("conf.input.order.path");
		MultipleInputs.addInputPath(job, new Path(readPath), TextInputFormat.class, ReadResultInputMapper.class);
		MultipleInputs.addInputPath(job, new Path(viewPath), TextInputFormat.class, ViewResultInputMapper.class);
		MultipleInputs.addInputPath(job, new Path(orderPath), TextInputFormat.class, OrderResultInputMapper.class);
		
		String outputPath = properties.getProperty("conf.output.path");
		
		//在执行新的job之前先把上一次的输出目录进行删除
		rmr(outputPath,conf);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
}


/*public class CorrelationResultMergeTool extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		job.setJarByClass(CorrelationResultMergeTool.class);
		job.setJobName("correlation result merge");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(ResultMergeReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReadResultInputMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ViewResultInputMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, OrderResultInputMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new CorrelationResultMergeTool(), args);
		System.exit(ret);
	}
}*/
