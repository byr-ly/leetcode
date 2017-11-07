package com.eb.bi.rs.mras2.bookrec.qiangfarec.fillersort;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class FillerZoneDirver extends BaseDriver{
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("FillerZoneDirver");
		long start = System.currentTimeMillis();
		
		Configuration conf = new Configuration(getConf());
		
		String value;
		if ((value = properties.getProperty("field.delimiter"))!= null) {
			conf.set("field.delimiter", value);
		}
		if ((value = properties.getProperty("field.delimiter.1")) != null) {
			conf.set("field.delimiter.1", value);
		}
		if ((value = properties.getProperty("field.delimiter.2")) != null) {
			conf.set("field.delimiter.2", value);
		}
		if ((value = properties.getProperty("rec.book.edit.point")) != null) {
			conf.set("rec.book.edit.point", value );
		}
		if ((value = properties.getProperty("new.book.edit.point")) != null) {
			conf.set("new.book.edit.point", value );
		}
		
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}	
		
		String areaBookCachePath = properties.getProperty("hdfs.areabook.cache.path");
		if (areaBookCachePath == null){
			throw new RuntimeException("area book cache path is essential");
		}

		Job job = new Job(conf);

		FileSystem fs1 = FileSystem.get(conf);	
		FileStatus[] status1 = fs1.globStatus(new Path(areaBookCachePath));
		for(int i = 0;  i  < status1.length; i++){
			//DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf);
			job.addCacheFile(URI.create(status1[i].getPath().toString()));
			log.info("book_label_data file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}	
		

		job.setJarByClass(FillerZoneDirver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//输入输出相关配置
		String fillerPlanBookInfoInputPath = properties.getProperty("hdfs.filler.plan.bookinfo.input.path");
		if (fillerPlanBookInfoInputPath == null){
			throw new RuntimeException("filler plan bookinfo input path is essential");
		}
		log.info(fillerPlanBookInfoInputPath);
		MultipleInputs.addInputPath(job, new Path(fillerPlanBookInfoInputPath), TextInputFormat.class, FillerBookTopNPlanMapper.class);
		
		String fillerResultOutputPath = properties.getProperty("hdfs.filler.result.output.path");
		if(fillerResultOutputPath == null){
			throw new RuntimeException("filler result output path is essential");
		}
		check(fillerResultOutputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(fillerResultOutputPath));		

		job.setReducerClass(FillerZoneReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
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
