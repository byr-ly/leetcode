package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.hadoop.compression.lzo.LzoCodec;

public class CalculationOfRelationInCacheDriver extends BaseDriver{
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("CalculationOfRelationInCacheDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		String inputPath = app_conf.getProperty("hdfs.input.path");
		String outputPath = app_conf.getProperty("hdfs.output.path");
		String cachePath = app_conf.getProperty("hdfs.cache.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//分隔符配置
		String sys_separator = app_conf.getProperty("hadoop.io.k_v_separator");
		String field_separator = app_conf.getProperty("Appconf.data.field.separator");
		String inner_separator = app_conf.getProperty("Appconf.data.inner.separator");
		//--------------------------------------------------------
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", sys_separator);
		conf.set("mapred.textoutputformat.separator",sys_separator);
		
		conf.set("Appconf.data.field.separator",field_separator);
		conf.set("Appconf.data.inner.separator",inner_separator);
		//--------------------------------------------------------
		//内存数据加载
		FileSystem fs1 = FileSystem.get(conf);	
		FileStatus[] status1 = fs1.globStatus(new Path(cachePath + "/part-*"));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf);	
					
			log.info("book_similar file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
		
		job = new Job(conf);
		job.setJarByClass(CalculationOfRelationInCacheDriver.class);
		
		check(outputPath,conf);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
				
		//设置M-R
		job.setMapperClass(CalculationOfRelationInCacheMapper.class);
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(CalculationOfRelationInCacheReducer.class);
		
		//设置输入/输出格式
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//压缩配置
		SequenceFileOutputFormat.setCompressOutput(job, true);
		
		SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job,LzoCodec.class);
		
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
