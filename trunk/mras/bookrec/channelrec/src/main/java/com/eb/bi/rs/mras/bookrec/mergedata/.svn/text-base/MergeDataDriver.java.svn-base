package com.eb.bi.rs.mras.bookrec.mergedata;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.tools.ant.taskdefs.Rename;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class MergeDataDriver extends BaseDriver{
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		String allDataInputPath = properties.getProperty("all.data.input.path");//全量数据输入路径		
		conf.set("allDataInputPath", allDataInputPath);
		String incrementDataInputPath = properties.getProperty("increment.data.input.path");//增量数据输入路径
		conf.set("incrementDataInputPath", incrementDataInputPath);
		String outputPath = properties.getProperty("hdfs.output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}
		
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		
		MultipleInputs.addInputPath(job, new Path(allDataInputPath), TextInputFormat.class, MergeAllDataMapper.class);
		MultipleInputs.addInputPath(job, new Path(incrementDataInputPath), TextInputFormat.class, MergeIncrementDataMapper.class);
		job.setReducerClass(MergeDataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		if (job.waitForCompletion(true)) {
			check(allDataInputPath, conf);
			rename(outputPath , allDataInputPath , conf);
			return 0;
		}else {
			return -1;
		}
	}
	
	private void rename(String outputPath, String allDataInputPath , Configuration conf) {
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.rename(new Path(outputPath), new Path(allDataInputPath));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}  
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
