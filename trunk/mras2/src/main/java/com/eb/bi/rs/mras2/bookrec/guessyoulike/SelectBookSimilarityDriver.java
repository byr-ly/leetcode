package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class SelectBookSimilarityDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//业务相关配置
		String value;
		if ((value = properties.getProperty("select.max.number")) != null) {
			conf.setInt("select.max.number", Integer.parseInt(value));
		}	
		
		if ((value = properties.getProperty("select.min.number")) != null) {
			conf.setInt("select.min.number", Integer.parseInt(value));
		}	
		
		if ((value = properties.getProperty("similarity.index")) != null) {
			conf.set("similarity.index", value);
		}
		
		if ((value = properties.getProperty("complementary.confidence")) != null) {
			conf.setFloat("complementary.confidence", Float.parseFloat(value));
		}
		//mr配置相关
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}
		
		//输入输出相关配置		
		//缓存
		String recRepo = properties.getProperty("recommend.repository");
		if(recRepo == null){
			throw new RuntimeException("recommend repository is essential");
		}	
		
		Job job = Job.getInstance(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		FileSystem fs = FileSystem.get(URI.create(recRepo), conf);
		FileStatus[] status = fs.listStatus(new Path(recRepo));
		
		for (FileStatus st:status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}

		String inputPaths = properties.getProperty("input.path");
		if(inputPaths == null){
			throw new RuntimeException("input path is essential");
		}
		String outputPath = properties.getProperty("output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}

		
		job.setMapperClass(SelectBookSimilarityMapper.class);
		job.setReducerClass(SelectBookSimilarityReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BookInfo.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
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
