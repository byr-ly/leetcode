package com.eb.bi.rs.mras.bookrec.qiangfarec.filler;

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

public class SelectHighlyRecommendBooksFillerDriver extends BaseDriver {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();		
		
		String fillerNumber = properties.getProperty("filler.number");
		conf.set("filler.number", fillerNumber);
		
		String topBookNumber = properties.getProperty("top.book.number");
		conf.set("top.book.number", topBookNumber);
		
		String highlyRecBookNum = properties.getProperty("highly.recommend.book.number");
		conf.set("highly.recommend.book.number", highlyRecBookNum);
		
		String inputPaths = properties.getProperty("hdfs.input.path");
		if(inputPaths == null){
			throw new RuntimeException("input path is essential");
		}
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
		job.setMapperClass(SelectHighlyRecommendBooksFillerMapper.class);
		job.setReducerClass(SelectHighlyRecommendBooksFillerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		MultipleOutputs.addNamedOutput(job, SelectHighlyRecommendBooksFillerReducer.HIGHLYREC, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, SelectHighlyRecommendBooksFillerReducer.TOPREC, TextOutputFormat.class, NullWritable.class, Text.class);
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
