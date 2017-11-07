package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class OrderedTopNOrderByMultiFieldDriver extends BaseDriver{

	@Override
	public int run(String[] args) throws Exception {		
		
		Configuration conf = getConf();
		String value;
		if ((value = properties.getProperty("field.delimiter"))!= null) {
			conf.set("field.delimiter", value);
		}
		if ((value = properties.getProperty("key.field.indexes")) != null) {
			conf.set("key.field.indexes", value);
		}		
		if ((value = properties.getProperty("top.number")) != null) {
			conf.setInt("top.number", Integer.parseInt(value) );
		}		
		if ((value = properties.getProperty("low.bound")) != null) {
			conf.setInt("low.bound", Integer.parseInt(value) );
		}	
		if ((value = properties.getProperty("order.mode")) == null) {
			throw new RuntimeException("order mode is essential");
		}else {
			conf.set("order.mode", value);
		}		
		//输出格式相关配置
		if ((value = properties.getProperty("is.vertical")) != null) {
			conf.setBoolean("is.vertical", Boolean.parseBoolean(value));
		}
		if ((value = properties.getProperty("with.sequence")) != null) {
			conf.setBoolean("with.sequence", Boolean.parseBoolean(value));
		}
		if ((value = properties.getProperty("reserve.field.indexes")) != null) {
			conf.set("reserve.field.indexes", value);
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
		
		if ( (value = properties.getProperty("mapreduce.inputformat.class")) != null){
			conf.setClass("mapreduce.inputformat.class", Class.forName(value), InputFormat.class);
		}		
		if ( (value = properties.getProperty("mapreduce.outputformat.class")) != null){
			conf.setClass("mapreduce.outputformat.class", Class.forName(value), OutputFormat.class);
		}		
		if ( (value = properties.getProperty("mapred.output.compress")) != null){
			conf.setBoolean("mapred.output.compress", Boolean.parseBoolean(value));
		}		
		if( (value = properties.getProperty("mapred.output.compression.codec")) != null){
			conf.setClass("mapred.output.compression.codec", Class.forName(value), CompressionCodec.class);
		}		
		if( (value = properties.getProperty("mapred.output.compression.type")) != null){
			 conf.set("mapred.output.compression.type", value);
		}
		
		
		Job job = new Job(conf, getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		job.setMapperClass(OrderedTopNOrderByMultiFieldMapper.class);
		job.setCombinerClass(OrderedTopNOrderByMultiFieldCombiner.class);		
		job.setReducerClass(OrderedTopNOrderByMultiFieldReducer.class);		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		
		return job.waitForCompletion(true) ? 0:1;
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

