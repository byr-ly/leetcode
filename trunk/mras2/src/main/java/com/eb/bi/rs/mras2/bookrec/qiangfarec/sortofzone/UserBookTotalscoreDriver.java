package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class UserBookTotalscoreDriver extends BaseDriver{
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("SortBookEditPointsDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf = new Configuration(getConf());
		
		String value;
		if ((value = properties.getProperty("field.delimiter"))!= null) {
			conf.set("field.delimiter", value);
		}
		if ((value = properties.getProperty("field.delimiter.1"))!= null) {
			conf.set("field.delimiter.1", value);
		}
		if ((value = properties.getProperty("field.delimiter.2"))!= null) {
			conf.set("field.delimiter.2", value);
		}
		if ((value = properties.getProperty("his.book.point")) != null) {
			conf.set("his.book.point", value );
		}
		if ((value = properties.getProperty("cache.info.length")) != null) {
			conf.setInt("cache.info.length", Integer.parseInt(value) );
		}
		
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}

		job = new Job(conf);

		String cachePath = properties.getProperty("hdfs.bookinfo.edit.points.cache.path");
		FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(cachePath));
        for (int i = 0; i < fs.length; i++) {
            //DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(fs[i].getPath().toString()));
            log.info(fs[i].getPath().toString() + " has been add into distributedCache");
        }

		job.setJarByClass(UserBookTotalscoreDriver.class);

		//输入输出相关配置		
		String userbookPrefersocreInputPath = properties.getProperty("hdfs.userbook.prefersocre.input.path");
		if (userbookPrefersocreInputPath == null) {
			throw new RuntimeException("userbook prefersocre input path is essential");
		} 
		MultipleInputs.addInputPath(job, new Path(userbookPrefersocreInputPath), TextInputFormat.class, UserBookPrefersocreMapper.class);			
		
		String userHistoryBookInputPath = properties.getProperty("hdfs.user.history.book.input.path");
		if (userHistoryBookInputPath == null){
			throw new RuntimeException("user history book input path is essential");
		}
		MultipleInputs.addInputPath(job, new Path(userHistoryBookInputPath), TextInputFormat.class, UserHistoryBookMapper.class);
	
		String userbookTotalPointsOutputPath = properties.getProperty("hdfs.userbook.totalpoints.output.path");
		if(userbookTotalPointsOutputPath == null){
			throw new RuntimeException("user book total points output path is essential");
		}
		check(userbookTotalPointsOutputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(userbookTotalPointsOutputPath));		
		
			
		job.setReducerClass(UserBookTotalscoreReducer.class);
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
