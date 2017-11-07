package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import com.hadoop.compression.lzo.LzoCodec;


public class UserMaxPrefsDriver extends BaseDriver{
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String value;
		if( (value = properties.getProperty("select.similarity.count")) != null){
			conf.setInt("select.similarity.count", Integer.parseInt(value));
		}
		
		if( (value = properties.getProperty("select.property.count")) != null){
			conf.setInt("select.property.count", Integer.parseInt(value));
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
		String reduceNum = properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}
		
		Job job = Job.getInstance(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		job.setMapperClass(UserMaxPrefsMapper.class);
		job.setCombinerClass(UserMaxPrefsCombiner.class);
		job.setReducerClass(UserMaxPrefsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		return job.waitForCompletion(true) ? 0 : -1;

	}

	
	public void check(String path, Configuration conf) {		
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}       
    }
}
