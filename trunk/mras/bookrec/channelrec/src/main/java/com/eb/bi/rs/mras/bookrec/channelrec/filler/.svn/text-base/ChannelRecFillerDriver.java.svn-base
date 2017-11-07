package com.eb.bi.rs.mras.bookrec.channelrec.filler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class ChannelRecFillerDriver extends BaseDriver {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();		
		
		String fillerNumber = properties.getProperty("filler.number");
		conf.set("fillerNumber", fillerNumber);
		
		String fillerBooksNumber = properties.getProperty("filler.books.number");
		conf.set("fillerBooksNumber", fillerBooksNumber);
		
		String inputPaths = properties.getProperty("hdfs.input.path");
		if(inputPaths == null){
			throw new RuntimeException("input path is essential");
		}
		String outputPath = properties.getProperty("hdfs.output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}		
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setNumReduceTasks(1);
		job.setMapperClass(ChannelRecFillerMapper.class);
		job.setReducerClass(ChannelRecFillerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public void check(String path, Configuration conf) 
	{		
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}       
       
    }
}
