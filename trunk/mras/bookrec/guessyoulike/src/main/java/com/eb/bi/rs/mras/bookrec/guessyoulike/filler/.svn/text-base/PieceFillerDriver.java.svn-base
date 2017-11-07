package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class PieceFillerDriver extends BaseDriver {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("PieceFillerDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		String inputPath = app_conf.getProperty("hdfs.input.path");
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		//int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//应用配置
		String recommendNum = app_conf.getProperty("Appconf.piecefiller.recommendnum","12");
		String randomNum = app_conf.getProperty("Appconf.piecefiller.randomnum","500");
		
		String Ways = app_conf.getProperty("Appconf.random.way");
		String type = app_conf.getProperty("Appconf.book.type");
		
		//--------------------------------------------------------
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//应用配置
		conf.set("Appconf.piecefiller.recommendnum",recommendNum);
		conf.set("Appconf.piecefiller.randomnum",randomNum);
		
		conf.set("Appconf.random.way",Ways);
		conf.set("Appconf.book.type",type);
		//--------------------------------------------------------
		//job-setup
		job = new Job(conf);
		job.setJarByClass(PieceFillerDriver.class);
		//检查输出目录是否存在
		check(outputPath,conf);
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setMapperClass(PieceFillerMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(PieceFillerReducer.class);
		//设置输入/输出格式
		job.setInputFormatClass(TextInputFormat.class);
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
