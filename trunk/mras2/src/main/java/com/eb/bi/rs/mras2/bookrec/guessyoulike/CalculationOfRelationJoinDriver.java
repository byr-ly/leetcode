package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.Properties;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.log4j.Logger;


/**
 * 关联计算jion_map版
 * */
public class CalculationOfRelationJoinDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("CalculationOfRelationJoinDriver");
		long start = System.currentTimeMillis();
		
		//Job job = null;
		
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		String userPath = app_conf.getProperty("hdfs.input.path");
		String bookPath = app_conf.getProperty("hdfs.cache.path");
		
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//分隔符配置
		String sys_separator = app_conf.getProperty("hadoop.io.k_v_separator");
		//--------------------------------------------------------
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", sys_separator);
		conf.set("mapred.textoutputformat.separator",sys_separator);		
		//--------------------------------------------------------		
		///join版关联计算
		conf.set("mapred.join.expr", CompositeInputFormat.compose("inner", SequenceFileInputFormat.class,userPath,bookPath));
		Job jobConf = Job.getInstance(conf );
		check(outputPath,conf);
		//设置M-R
		jobConf.setJarByClass(CalculationOfRelationJoinDriver.class); 
		jobConf.setMapperClass(CalculationOfRelationJoinMapper.class);
		jobConf.setNumReduceTasks(reduceNum);
		jobConf.setReducerClass(CalculationOfRelationJoinReducer.class);
		
		jobConf.setInputFormatClass(CompositeInputFormat.class); 
		//jobConf.set("mapred.join.expr", CompositeInputFormat.compose("inner", SequenceFileInputFormat.class,userPath,bookPath));

		FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
		jobConf.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//设置输出类型(map)
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		jobConf.setOutputKeyClass(NullWritable.class);
		jobConf.setOutputValueClass(Text.class);
		
		//日志==================================================================================
		
		if( jobConf.waitForCompletion(true)){
			log.info("job[CalculationOfRelationInCacheDriver] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[CalculationOfRelationInCacheDriver] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
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
