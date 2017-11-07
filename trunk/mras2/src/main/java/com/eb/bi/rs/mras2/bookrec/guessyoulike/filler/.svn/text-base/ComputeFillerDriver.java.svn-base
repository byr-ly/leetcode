package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class ComputeFillerDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("ComputeFillerDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		//待补白结果
		String inputPath1 = app_conf.getProperty("hdfs.input.path.result");
		//过滤数据
		String inputPath2 = app_conf.getProperty("hdfs.input.path.black","");
		//补白库
		String cachePath = app_conf.getProperty("hdfs.cache.path.bookdb");
		//输出
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//应用配置
		String recommendMim = app_conf.getProperty("Appconf.filler.minnum","4");
		String recommendMax = app_conf.getProperty("Appconf.filler.maxnum","12");
		//--------------------------------------------------------
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//应用配置
		conf.set("Appconf.filler.minnum",recommendMim);
		conf.set("Appconf.filler.maxnum",recommendMax);
		//--------------------------------------------------------
		//job-setup
		job = Job.getInstance(conf);
		job.setJarByClass(ComputeFillerDriver.class);
		//补白库加载
		FileSystem fs1 = FileSystem.get(conf);
		FileStatus[] status1 = fs1.globStatus(new Path(cachePath));
		for(FileStatus st : status1){
			job.addCacheFile(URI.create(st.getPath().toString()));
			log.info("book_fillter_data file: " + st.getPath().toString() + " has been add into distributed cache");
		}
		//检查输出目录是否存在
		check(outputPath,conf);
		//待补白结果(不能没有数据)
		MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, ComputeFillerMapper1.class);
		//黑名单(可以没有)
		String[] inputPaths = inputPath2.split(";");
		if(!inputPath2.equals("")){
			for(int i = 0;i != inputPaths.length;i++){
				MultipleInputs.addInputPath(job, new Path(inputPaths[i]), TextInputFormat.class, ComputeFillerMapper2.class);
			}
		}
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(ComputeFillerReducer.class);
		//设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//reduce多种文件输出(不同输出格式)
		//MultipleOutputs.addNamedOutput(job,"Vertical",TextOutputFormat.class,NullWritable.class,Text.class);
		//MultipleOutputs.addNamedOutput(job,"Horizontal",TextOutputFormat.class,NullWritable.class,Text.class);
		
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");			

		return 0;
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
