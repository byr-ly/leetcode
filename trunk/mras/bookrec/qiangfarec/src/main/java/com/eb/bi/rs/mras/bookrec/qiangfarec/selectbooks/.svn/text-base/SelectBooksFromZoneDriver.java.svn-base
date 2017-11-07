package com.eb.bi.rs.mras.bookrec.qiangfarec.selectbooks;

import java.io.IOException;
import java.net.URI;

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

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
public class SelectBooksFromZoneDriver extends BaseDriver{

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		//缓存
		String bookInfo = properties.getProperty("hdfs.books.info.path");
		if(bookInfo == null){
			throw new RuntimeException("books info is essential");
		}		
		FileSystem fs = FileSystem.get(URI.create(bookInfo), conf);
		FileStatus[] status = fs.listStatus(new Path(bookInfo));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
		}

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
		job.setNumReduceTasks(0);
		job.setMapperClass(SelectBooksFromZoneMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
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
