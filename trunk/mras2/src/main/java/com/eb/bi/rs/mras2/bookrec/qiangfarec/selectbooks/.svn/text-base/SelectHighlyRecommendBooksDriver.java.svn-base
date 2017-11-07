package com.eb.bi.rs.mras2.bookrec.qiangfarec.selectbooks;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;


public class SelectHighlyRecommendBooksDriver extends BaseDriver {
	@Override
	public int run(String[] arg0) throws Exception {

		Logger log = Logger.getLogger("SelectHighlyRecommendBooksDriver");
		Configuration conf = getConf();

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

		//缓存	
		String topBookNumber = properties.getProperty("top.book.number");
		conf.set("top.book.number", topBookNumber);
		
		String highlyRecBookNum = properties.getProperty("highly.recommend.book.number");
		conf.set("highly.recommend.book.number", highlyRecBookNum);
		
		String bookClassifiedInfo = properties.getProperty("hdfs.books.classify.info.path");
		if(bookClassifiedInfo == null){
			throw new RuntimeException("books classify info is essential");
		}
		conf.set("books.classify.info", bookClassifiedInfo);

		String classMapdepartment = properties.getProperty("hdfs.class.department.path");
		if(classMapdepartment == null){
			throw new RuntimeException("class map department info is essential");
		}
		conf.set("class.department", classMapdepartment);

		Job job = new Job(conf,getClass().getSimpleName());

		FileSystem fs = FileSystem.get(URI.create(bookClassifiedInfo), conf);
		FileStatus[] status = fs.globStatus(new Path(bookClassifiedInfo + "part*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info(status[i].getPath().toString() + " has been add into distributedCache");
		}

		fs = FileSystem.get(URI.create(classMapdepartment), conf);
		status = fs.listStatus(new Path(classMapdepartment));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info(status[i].getPath().toString() + " has been add into distributedCache");
		}

		job.setJarByClass(getClass());

		job.setMapperClass(SelectHighlyRecommendBooksMapper.class);
		job.setReducerClass(SelectHighlyRecommendBooksReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		MultipleOutputs.addNamedOutput(job, SelectHighlyRecommendBooksReducer.HIGHLYREC, TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, SelectHighlyRecommendBooksReducer.TOPREC, TextOutputFormat.class, Text.class, NullWritable.class);
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
