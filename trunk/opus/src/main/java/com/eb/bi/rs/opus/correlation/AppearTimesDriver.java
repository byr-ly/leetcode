package com.eb.bi.rs.opus.correlation;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class AppearTimesDriver extends BaseDriver{
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		String reducerNum =  properties.getProperty("reducer.num");
		if(reducerNum != null){
			conf.set("mapred.reduce.tasks", reducerNum);
		}

		Job job = Job.getInstance(conf,getClass().getSimpleName());
		
		String inputPath = properties.getProperty("hdfs.input.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		
		String outputPath = properties.getProperty("hdfs.output.path");
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
