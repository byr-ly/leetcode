package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.StringDoublePair;


public class SelectFinalRecResultDriver extends BaseDriver{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//业务相关配置
		String value;
		if ((value = properties.getProperty("select.number")) != null) {
			conf.setInt("select.number", Integer.parseInt(value));
		}
		
		//输入输出相关配置
		String inputPaths = properties.getProperty("input.path");
		if(inputPaths == null){
			throw new RuntimeException("input path is essential");
		}
		String outputPath = properties.getProperty("output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}
		//mr配置相关
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}
		
		
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		
		job.setMapperClass(SelectFinalRecResultMapper.class);
		job.setCombinerClass(SelectFinalRecResultCombiner.class);
		job.setReducerClass(SelectFinalRecResultReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringDoublePair.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
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


