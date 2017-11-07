package com.eb.bi.rs.opus.correlation;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class DataPrepDriver extends BaseDriver{
	@Override
	public int run(String[] arg0) throws Exception {
		Logger log = Logger.getLogger("CalculateIndexDriver");
		Configuration conf ;
		Job job;
		FileStatus[] status;
		long start;
		String reducerNum =  properties.getProperty("reducer.num");
		String inputPath = properties.getProperty("hdfs.input.path");
		/*
		 * 数据合并
		 */
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		conf = new Configuration(getConf());
//		conf.set("mapreduce.job.queuename", "queue_eb");
		if(reducerNum != null){
			conf.set("mapred.reduce.tasks", reducerNum);
		}

		job = Job.getInstance(conf,"data merge");
		
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		String outputPath = properties.getProperty("hdfs.output.path1");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("output path is essential");
		}

		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setMapperClass(DataPrepMapper.class);
		job.setReducerClass(DataPrepReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		check(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}	
		Counters counters = job.getCounters();
		Long record_num = counters.findCounter("RECORD", "NUM").getValue();
		log.info("record_num : " + record_num );
		
		/*
		 * 数据初步处理
		 */
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		conf = new Configuration(getConf());

		reducerNum =  properties.getProperty("reducer.num");
		if(reducerNum != null){
			conf.set("mapred.reduce.tasks", reducerNum);
		}
		conf.set("record_num", record_num.toString());
		job = Job.getInstance(conf , getClass().getSimpleName());
		
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		
		outputPath = properties.getProperty("hdfs.output.path2");
		if (StringUtils.isBlank(outputPath)) {
			throw new RuntimeException("output path is essential");
		}

		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setMapperClass(AppearTimesMapper.class);
		job.setReducerClass(AppearTimesReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
