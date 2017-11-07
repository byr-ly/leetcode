package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark.basetime;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
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

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class BaseTimeDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("BaseTimeDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		//用户图书打分结果
		String inputPath1 = app_conf.getProperty("hdfs.input.path.1");
		//用户图书阅读日期
		String inputPath2 = app_conf.getProperty("hdfs.input.path.2");
		//输出根目录
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//应用配置
		String h1 = app_conf.getProperty("hdfs.app.conf.h1","12");
		String M = app_conf.getProperty("hdfs.app.conf.M","60");
		//--------------------------------------------------------
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//应用配置
		conf.set("hdfs.app.conf.h1", h1);
		conf.set("hdfs.app.conf.M", M);
		//--------------------------------------------------------
		//job-setup		
		job = Job.getInstance(conf);
		job.setJarByClass(BaseTimeDriver.class);
		//检查输出目录是否存在
		check(outputPath,conf);
		
		//用户图书打分数据
		MultipleInputs.addInputPath(job, new Path(inputPath1), 
				TextInputFormat.class, UesrBookMarkMapper.class);
		
		//用户图书访问时间数据
		MultipleInputs.addInputPath(job, new Path(inputPath2), 
				TextInputFormat.class, UserBookRecordMapper.class);
		
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(BaseTimeReducer.class);
		//设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
				
		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");

		//在最终结果中添加lastday
		conf = new Configuration(getConf());

		app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		//最终用户图书打分结果
		inputPath1 = app_conf.getProperty("hdfs.output.path");
		//用户图书阅读日期
		inputPath2 = app_conf.getProperty("hdfs.input.path.2");
		//输出根目录
		outputPath = app_conf.getProperty("hdfs.output.path.lastday");
		//并行度配置
		reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));
		maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//job-setup
		job = new Job(conf);
		job.setJarByClass(BaseTimeDriver.class);
		//检查输出目录是否存在
		check(outputPath, conf);

		//最终用户图书打分数据
		MultipleInputs.addInputPath(job, new Path(inputPath1),
				TextInputFormat.class, BookFinalScoreMapper.class);

		//用户图书访问时间数据
		MultipleInputs.addInputPath(job, new Path(inputPath2),
				TextInputFormat.class, BookFinalScoreMapper.class);

		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(AddLastDayReducer.class);
		//设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);

		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//日志==================================================================================
		if (job.waitForCompletion(true)) {
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		} else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}

		return 0;
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
