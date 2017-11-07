package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

public class ClusteringDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Logger log = Logger.getLogger("ClusteringDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		//用户推荐结果
		String inputPath1 = app_conf.getProperty("hdfs.input.path.result");
		//用户推荐结果数据格式
		String input1Format = app_conf.getProperty("hdfs.input.format.result");
		//用户群体信息
		String inputPath2 = app_conf.getProperty("hdfs.input.path.clustering");
		
		//用户群体信息数据格式
		//String input2Format = app_conf.getProperty("hdfs.input.format.clustering");
		
		//输出根目录
		String outputPath = app_conf.getProperty("hdfs.output.path");
		
		//输出数据格式
		//String outputFormat = app_conf.getProperty("hdfs.output.format");
		
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//应用配置
		//得分群数据key.index
		String keyIndex = app_conf.getProperty("Appconf.key.index");
		//分群方式
		String clusteringClass = app_conf.getProperty("hdfs.conf.compute.clusteringclass","1");
		//--------------------------------------------------------
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//应用配置
		conf.set("Appconf.key.index", keyIndex);
		conf.set("hdfs.conf.compute.clusteringclass", clusteringClass);
		//--------------------------------------------------------
		//job-setup
		job = Job.getInstance(conf);
		job.setJarByClass(ClusteringDriver.class);
		//检查输出目录是否存在
		check(outputPath,conf);
				
		//推荐结果导入
		if (inputPath1 == null) {
			throw new RuntimeException("to user recommend input path is essential");
		} 
		if (input1Format == null) {
			throw new RuntimeException("to user recommend inputformat is essential");
		}
		
		MultipleInputs.addInputPath(job, new Path(inputPath1), 
				Class.forName(input1Format).asSubclass(InputFormat.class), ClusteringMapper1.class);
		
		//用户群体信息导入
		MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, ClusteringMapper2.class);
		
		//设置reducer分区规则
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(ClusteringReducer.class);
		//设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输出类型(map)
		job.setMapOutputKeyClass(TextPair.class);
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
	
	public void check(String path, Configuration conf) {
		try {	
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	public static class KeyPartition extends Partitioner<TextPair, Text>{
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}		
	}
}
