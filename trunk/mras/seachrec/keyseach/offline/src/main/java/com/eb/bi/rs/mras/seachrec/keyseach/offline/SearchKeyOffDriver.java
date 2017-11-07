package com.eb.bi.rs.mras.seachrec.keyseach.offline;


import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import  com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import  com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import  com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import  com.eb.bi.rs.frame.common.pluginutil.PluginUtil;



public class SearchKeyOffDriver extends Configured  implements Tool{
	private static PluginUtil pluginUtil;
	private static Logger log;
	
	
	public SearchKeyOffDriver(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}

	public static void main(String[] args) throws Exception {
		
		Date dateBeg = new Date();
		int ret = ToolRunner.run(new Configuration(), new SearchKeyOffDriver(args), args);	

		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(dateEnd);		
		long timeCost = dateEnd.getTime() - dateBeg.getTime();		

		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();
		
		log.info("time cost in total(ms) :" + timeCost) ;
		System.exit(ret);	

	}

	@Override
	public int run(String[] args) throws Exception {		
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		FileSystem fs;
		Job job;
		long start;	
		
		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：当前统计周期内的用户搜索历史记录（增量）
		 **		2.缓存数据：停用词
		 ** 输出：
		 *		当前统计周期内的搜索关键词词频数据
		 ** 功能描述：
		 ** 	1.分词(使用结巴分词)
		 **		2.去除停用词
		 **		3.计算搜获关键词词频

		 ****************************************************/
		
		log.info("job start to compute current keyword frequency");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());			
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
		conf.set("mapred.textoutputformat.separator","|");		

		/*停用词是一个文件*/	
		String stopWordPath = config.getParam("stopword_path", "");
		DistributedCache.addCacheFile(URI.create(stopWordPath), conf);
		log.info("stopword file: " + stopWordPath + " has been add into distributed cache");		
		

		job = new Job(conf);
		job.setJarByClass(SearchKeyOffDriver.class);		
		String searchRecordDir = config.getParam("search_record_dir", "");
		String currentKeyWordDir = config.getParam("current_keyword_dir", "");
		check(currentKeyWordDir);
		
		FileInputFormat.setInputPaths(job, new Path(searchRecordDir));
		FileOutputFormat.setOutputPath(job, new Path(currentKeyWordDir));
		log.info("job input path: " + searchRecordDir);
		log.info("job output path: " + currentKeyWordDir);		
		
		job.setMapperClass(SearchRecordToKeyWordMapper.class);	
		job.setReducerClass(SearchRecordToKeyWordReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		
		
		log.info("=================================================================================");		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：当前统计周期内的搜索关键词词频数据
		 **		2.输入路径：历史搜索关键词词频数据
		 ** 输出：
		 *		1.汇总后的搜索关键词词频，作为新的历史搜索关键词词频数据。
		 ** 功能描述：
		 ** 	1.汇总当前统计周期的搜索关键词词频以及历史搜索关键词词频
		 **		2.去除低频词(保留词频>=2)

		 ****************************************************/
		log.info("job start to aggregate keyword frequency");	
		start = System.currentTimeMillis();		

		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
		conf.setInt("reserved.low.frequency.keyword.value", config.getParam("reserved_low_frequency_keyword_value", 2));

		job = new Job(conf);
		job.setJarByClass(SearchKeyOffDriver.class);
		
		String historyKeyWordDir = config.getParam("history_keyword_dir", "");	
		if(historyKeyWordDir.endsWith("/")){
			historyKeyWordDir = historyKeyWordDir.substring(0, historyKeyWordDir.length() - 1);			
		}
		fs = FileSystem.get(conf);
		if(!fs.exists(new Path(historyKeyWordDir))){			
			FSDataOutputStream out = fs.create(new Path(historyKeyWordDir + "/total/part-r-00000"));
			out.close();
		}
		FileInputFormat.addInputPath(job, new Path(historyKeyWordDir + "/total/part-*"));
		
		if(currentKeyWordDir.endsWith("/")){
			currentKeyWordDir = currentKeyWordDir.substring(0, currentKeyWordDir.length() - 1);	
		}
		FileInputFormat.addInputPath(job, new Path(currentKeyWordDir + "/part-*"));
		
		String aggregatedKeyWordDir = config.getParam("aggregated_keyword_dir", "");
		check(aggregatedKeyWordDir);
		FileOutputFormat.setOutputPath(job, new Path(aggregatedKeyWordDir));
		log.info("job input path: " + historyKeyWordDir + "/total");
		log.info("job input path: " + currentKeyWordDir);
		log.info("job output path: " + aggregatedKeyWordDir);		
		
		job.setMapperClass(AggregateKeyWordFrequencyMapper.class);	
		job.setReducerClass(AggregateKeyWordFrequencyReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			
			fs.delete(new Path(historyKeyWordDir), true);			
			fs.rename(new Path(aggregatedKeyWordDir), new Path(historyKeyWordDir));
			
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}	
		
		log.info("=================================================================================");	
		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		输入路径：图书信息数据
		 **     缓存数据：汇总后的搜索关键词词频（上一个JOB的输出）
		 ** 输出：
		 *		搜索关键词关联的标签词典，即搜索关键词标签权重表
		 ** 功能描述：
		 ** 	搜索关键词转换成关联标签

		 ****************************************************/
			
		log.info("job start to compute tag weight");
		start = System.currentTimeMillis();	
		
		conf = new Configuration(getConf());

		int maxSplitSize = config.getParam("tagdict_max_split_size", 64);
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSize));

		conf.set("mapred.textoutputformat.separator","|");
		conf.set("three.tag.weight", config.getParam("three_tag_weight", ""));
		conf.set("four.tag.weight", config.getParam("four_tag_weight", ""));
		conf.set("general.tag.weight", config.getParam("general_tag_weight", ""));
		if(historyKeyWordDir.endsWith("/")){
			historyKeyWordDir = historyKeyWordDir.substring(0, historyKeyWordDir.length() - 1);			
		}
		FileStatus[] status = fs.globStatus(new Path(historyKeyWordDir + "/filtered/part-*"));
		for(int i = 0; i < status.length; i++){
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	
			log.info("keyword frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}

	
		job = new Job(conf);
		job.setJarByClass(SearchKeyOffDriver.class);
	
		String bookRecordDir = config.getParam("book_info_path", "");
		String tagWeightDir = config.getParam("tag_weight_dir", "");
		check(tagWeightDir);
		FileInputFormat.setInputPaths(job, new Path(bookRecordDir));
		FileOutputFormat.setOutputPath(job, new Path(tagWeightDir));
		log.info("job input path: " + bookRecordDir);
		log.info("job output path: " + tagWeightDir);		
		
		job.setMapperClass(KeyWordToTagDictMapper.class);
		job.setCombinerClass(KeyWordToTagDictReducer.class);
		job.setReducerClass(KeyWordToTagDictReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		log.info("=================================================================================");	
		
		/*********************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		输入路径：搜索关键词标签权重表（上一个JOB的输出）
		 **     缓存数据：特殊标签表	
		 ** 输出：
		 *		权重排在前K名的标签的权重的归一化后的值（每个搜索关键词而言）
		 ** 功能描述：
		 ** 	1.将标签词典中的无用标签剔除掉
		 **     2.选取权重值排在前K名的标签 
		 **     3.对权重排在前K名的标签的权重求和
		 **     4.对权重排在前K名的标签的权重进行归一化处理
		 *

		 *********************************************************/

		log.info("job start to compute topk tag weight");	
		start = System.currentTimeMillis();	
		conf = new Configuration(getConf());	
		String specialTagPath= config.getParam("special_tag_path", "");		
		DistributedCache.addCacheFile(URI.create(specialTagPath), conf);
		log.info("special tag file: " +  specialTagPath + " has been add into distributed cache");
		conf.setInt("topk.number", config.getParam("topk_number", 5));
		
		job = new Job(conf);
		job.setJarByClass(SearchKeyOffDriver.class);
		
		if(tagWeightDir.endsWith("/")){
			tagWeightDir = tagWeightDir.substring(0, tagWeightDir.length() - 1);			
		}		
		FileInputFormat.setInputPaths(job, new Path(tagWeightDir + "/part-*"));

		String normalizedTopKTagWeightDir = config.getParam("normalized_topk_tag_weight_dir", "");
		check(normalizedTopKTagWeightDir);
		FileOutputFormat.setOutputPath(job, new Path(normalizedTopKTagWeightDir));	
		log.info("job input path: " + tagWeightDir);
		log.info("job output path: " + normalizedTopKTagWeightDir);
		
		job.setMapperClass(TopKTagDictMapper.class);		
		job.setReducerClass(TopKTagDictReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TagDictWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		
//		log.info("=================================================================================");	
//		/*********************************************************
//		 * MAP REDUCE JOB:
//		 ** 输入：
//		 **		输入路径：权重排在前K名的标签（对每个搜索关键词而言），即上一个JOB的输出
//		 ** 输出：
//		 *		权重排在前K名的标签的权重的和（每个搜索关键词而言）
//		 ** 功能描述：
//		 ** 	对权重排在前K名的标签的权重求和
//		 *********************************************************/
//		log.info("job start to compute topk tag weight sum");			
//		start = System.currentTimeMillis();
//		conf = new Configuration();
//		conf.set("mapred.textoutputformat.separator","|");
//		
//		job = new Job(conf);
//		job.setJarByClass(SearchKeyOffDriver.class);
//		if(topKTagWeightDir.endsWith("/")){
//			topKTagWeightDir = topKTagWeightDir.substring(0, topKTagWeightDir.length() -1);
//		}
//		
//		FileInputFormat.setInputPaths(job, new Path(topKTagWeightDir + "/part-*"));
//		String topKTagWeightSumDir = config.getParam("topk_tag_weight_sum_dir", "");  
//		check(topKTagWeightSumDir);
//		FileOutputFormat.setOutputPath(job, new Path(topKTagWeightSumDir));		
//		log.info("job input path: " + topKTagWeightDir);
//		log.info("job output path: " + topKTagWeightSumDir);		
//		
//		job.setMapperClass(TagDictSumMapper.class);		
//		job.setReducerClass(TagDictSumReducer.class);		
//
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		
//		if( job.waitForCompletion(true)){
//			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
//		}
//		else {
//			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
//			return 1;
//		}
//		
//		
//		log.info("=================================================================================");	
//		/*********************************************************
//		 * MAP REDUCE JOB:
//		 ** 输入：
//		 **		输入路径：权重排在前K名的标签（对每个搜索关键词而言），同上一个JOB的输入
//		 *		缓存数据：权重排在前K名的标签的权重的和（对每个搜索关键词而言），即上一个JOB的输出
//		 ** 输出：
//		 *		权重排在前K名的标签的权重的归一化后的值（每个搜索关键词而言）
//		 ** 功能描述：
//		 ** 	对权重排在前K名的标签的权重进行归一化处理
//		 *********************************************************/
//		log.info("job start to normalize topk tag weight");
//		conf = new Configuration();
//
//		if(topKTagWeightSumDir.endsWith("/")){
//			topKTagWeightSumDir = topKTagWeightSumDir.substring(0, topKTagWeightSumDir.length() - 1);			
//		}
//		status = fs.globStatus(new Path(topKTagWeightSumDir + "/part-*"));
//		for(int i = 0; i < status.length; i++){
//			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	
//			log.info("topk tag weight sum file: " + status[i].getPath().toString() + " has been add into distributed cache");
//		}		
//		
//		job = new Job(conf);
//		job.setJarByClass(SearchKeyOffDriver.class);	
//		
//		FileInputFormat.setInputPaths(job, new Path(topKTagWeightDir + "/part-*" ));
//		String normalizedTopKTagWeightDir = config.getParam("normalized_topk_tag_weight_dir", "");
//		check(normalizedTopKTagWeightDir);
//		FileOutputFormat.setOutputPath(job, new Path(normalizedTopKTagWeightDir));
//		
//		log.info("job input path: " + topKTagWeightDir);
//		log.info("job output path: " + normalizedTopKTagWeightDir);		
//		
//		
//		job.setMapperClass(NormalizeTagDictMapper.class);		
//		job.setNumReduceTasks(0);
//
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);
//		
//		if( job.waitForCompletion(true)){
//			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
//			return 0;
//		}
//		else {
//			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
//			return 1;
//		}		
	}
	
	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration());
			Path f = new Path(fileName);
			boolean isExists = fs.exists(f);
			if (isExists) {	//if exists, delete
				boolean isDel = fs.delete(f,true);
				log.info(fileName + "  delete?\t" + isDel);
			} else {
				log.info(fileName + "  exist?\t" + isExists);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
}
