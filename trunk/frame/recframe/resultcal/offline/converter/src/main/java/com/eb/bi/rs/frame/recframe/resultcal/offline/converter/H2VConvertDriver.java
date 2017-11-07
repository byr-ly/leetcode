package com.eb.bi.rs.frame.recframe.resultcal.offline.converter;

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

public class H2VConvertDriver extends BaseDriver{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();		

		String value;
		if( (value = properties.getProperty("single.multi.field.delimiter")) != null){
			conf.set("single.multi.field.delimiter", value);
		}
		
		if( (value = properties.getProperty("multi.group.delimiter")) != null){
			conf.set("multi.group.delimiter", value);
		}
		
		if( (value = properties.getProperty("multi.field.delimiter")) != null){
			conf.set("multi.field.delimiter", value);
		}
		
		if( (value = properties.getProperty("result.field.delimiter")) != null){
			conf.set("result.field.delimiter", value);
		}
		
		
		
		if( (value = properties.getProperty("single.field.indexes")) != null){
			conf.set("single.field.indexes", value);
		}
		if( (value = properties.getProperty("multi.unrelated.field.indexes")) != null){
			conf.set("multi.unrelated.field.indexes", value);
		}
		if( (value = properties.getProperty("multi.group.set.index")) != null){
			conf.setInt("multi.group.set.index", Integer.parseInt(value));
		}
		
		if( (value = properties.getProperty("reserved.multi.field.indexes")) != null){
			conf.set("reserved.multi.field.indexes", value);
		}
		
		if( (value = properties.getProperty("multi.group.contains.field.number")) != null){
			conf.setInt("multi.group.contains.field.number", Integer.parseInt(value));
		}
		
		
		if( (value = properties.getProperty("single.first")) != null){
			conf.setBoolean("single.first", Boolean.parseBoolean(value));
		}
		
		//mr配置相关
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
		
		
		
		//输入输出相关配置
		String inputPaths = properties.getProperty("input.path");
		if(inputPaths == null){
			throw new RuntimeException("input path is essential");
		}
		String outputPath = properties.getProperty("output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}
		
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
	
		job.setMapperClass(H2VConverterMapper.class);		
		job.setNumReduceTasks(0);
		
		
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
