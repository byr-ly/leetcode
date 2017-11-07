package com.eb.bi.rs.opus.correlation.filler;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class CorrelationRecFillerDriver extends BaseDriver {

	@Override
	public int run(String[] arg0) throws Exception {
		Logger log = Logger.getLogger("CalculateIndexDriver");
		Configuration conf = getConf();
		
		String recNum = properties.getProperty("rec.num");
		conf.set("rec.num", recNum);
		
		String filler_subject = properties.getProperty("filler.subject");
		conf.set("filler.subject", filler_subject);

		String recRanks =  properties.getProperty("rec.ranks");
		if(recRanks != null){
			conf.set("rec.ranks", recRanks);
		}
		
		String recSours =  properties.getProperty("rec.sours");
		if(recRanks != null){
			conf.set("rec.sours", recSours);
		}
		
		String reducerNum =  properties.getProperty("reducer.num");
		if(reducerNum != null){
			conf.set("mapred.reduce.tasks", reducerNum);
		}

		Job job = Job.getInstance(conf,getClass().getSimpleName());
		
		String cachePath = properties.getProperty("hdfs.cache.path");
		if(cachePath != null){
			conf.set("hdfs.cache.path", cachePath);
		}
		FileSystem fs = FileSystem.get(URI.create(cachePath), conf);
		FileStatus[] status = fs.listStatus(new Path(cachePath));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info(status[i].getPath().toString() + " has been add into distributedCache");
		}

		String inputPath = properties.getProperty("hdfs.input.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		
		String outputPath = properties.getProperty("hdfs.output.path");
		if (StringUtils.isBlank(outputPath)) {
			throw new RuntimeException("output path is essential");
		}
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setJarByClass(getClass());
		job.setMapperClass(CorrelationTypeMapper.class);
		job.setReducerClass(CorrelationRecFillerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		check(outputPath, conf);
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
