package com.eb.bi.rs.mras.bookrec.voicebook;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//import com.eb.bi.rs.mras.bookrec.voicebook.OrderCorelationRecDriver.KeyPartition;
import com.eb.bi.rs.mras.bookrec.voicebook.util.TextPair;

public class ListenCorelationRecDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Logger log = PluginUtil.getInstance().getLogger();
		PluginConfig config = PluginUtil.getInstance().getConfig();	
		
		Configuration conf ;
		Job job;
		FileStatus[] status;
		long start;
		
		
		/*
		 * ********************************************************************************************
		 * 
		 *                                   LISTEN ALSO LISTEN
		 * 
		 * ********************************************************************************************
		 */
		
		
		/*************************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：收听关联指标数据
		 **     2.缓存文件：图书分类信息
		 **     3.缓存文件：深度阅读频次数据
		 ** 输出：
		 **		满足一定条件约束的关联指标数据
		 ** 功能描述：
		 ** 	过滤关联指标数据，使其满足：
		 **		1.A、B图书同属于一个大类
		 **		2.B书的深度阅读用户数大于一定阈值
		 **     3。AB的共同用户数满足一定阈值（输入已经满足该条件）

		 *************************************************************************************************/
		log.info("========================================================================================");	
		start = System.currentTimeMillis();
		
		String bookInfoPath = config.getParam("book_info_path", "");
		String bookDeepListenFrequencyPath = config.getParam("book_deep_listen_frequency_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("deep.listen.threshold", config.getParam("listen_deep_listen_threshold", 50));		
		conf.set("book.info.path", bookInfoPath);
		conf.set("book.deep.listen.frequency.path", bookDeepListenFrequencyPath);
		
		status = FileSystem.get(conf).listStatus(new Path(bookInfoPath));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("book info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		
		status = FileSystem.get(conf).globStatus(new Path(bookDeepListenFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("deep listen frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		
		job = new Job(conf,"filter firsthand indicator data");		
		job.setJarByClass(getClass());		
		
		String listenFirsthandIndicatorPath = config.getParam("listen_firsthand_indicator_path", "");
		String listenFilteredIndicatorPath = config.getParam("listen_filtered_indicator_path", "");
		check(listenFilteredIndicatorPath);		

		FileInputFormat.setInputPaths(job, new Path(listenFirsthandIndicatorPath));	
		FileOutputFormat.setOutputPath(job, new Path(listenFilteredIndicatorPath));
		log.info("job input path: " + listenFirsthandIndicatorPath);
		log.info("job output path: " + listenFilteredIndicatorPath);		
		
		job.setMapperClass(ListenBookFilterMapper.class);
		job.setNumReduceTasks(0);	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}

		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：满足一定条件约束的关联指标数据
		 **     2.缓存文件：听书精品库数据
		 **     3.缓存文件：听书推荐库数据
		 ** 输出：
		 **		为存在关联指标数据的图书筛选关联图书
		 ** 功能描述：    
		 ** 	1.筛选规则
		 **       优先选择Class_type=1的图书， 若图书数不足，则选择class_type=2的图书
		 **     2.补白策略
		 *        听书精品库表中，在图书大类bigclassid下随机补白
		 *        如果仍然不足，则听书推荐库表中，在图书大类bigclassid下按照月访问用户量降序补足，补白图书无前置信度数据
		 **       

		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加百分比的上限和下限 
		float firstLowerBound = config.getParam("first_lower_bound", 0.1f);
		float firstUpperBound = config.getParam("first_upper_bound", 0.5f);
		//听书精品库数据缓存路径
		String bookListenQualityPath = config.getParam("book_listen_quality_path", "");
		//听书推荐库数据缓存路径
		String bookListenRecommendPath = config.getParam("book_listen_recommend_path", "");
		//听书系列图书数据缓存路径
		String bookSeriesPath = config.getParam("book_series_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("listen.corelation.recommend.number", config.getParam("listen_corelation_recommend_number", 40));		
		conf.set("book.listen.quality.path", bookListenQualityPath);
		conf.set("book.listen.recommend.path", bookListenRecommendPath);
		conf.set("book.series.path", bookSeriesPath);
		conf.setFloat("first.lower.bound", firstLowerBound);
		conf.setFloat("first.upper.bound", firstUpperBound);		
		conf.set("book.info.path", bookInfoPath);
		
		//将基本图书的信息添加到缓存
		status = FileSystem.get(conf).listStatus(new Path(bookInfoPath));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("book info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		//将听书精品库数据添加到缓存
		status = FileSystem.get(conf).globStatus(new Path(bookListenQualityPath + "/00*"));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("listen book quality file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		//将听书推荐库数据添加到缓存
		status = FileSystem.get(conf).globStatus(new Path(bookListenRecommendPath + "/00*"));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("listen book recommend file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		//将听书系列图书数据添加到缓存
		status = FileSystem.get(conf).globStatus(new Path(bookSeriesPath + "/00*"));
		for (int i = 0; i < status.length; i++) {
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("listen book series file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		job = new Job(conf, "listen corelation book recommend");
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(listenFilteredIndicatorPath));
		String listenCorelationBookRecPath = config.getParam("listen_corelation_book_recommend_path", "");
		check(listenCorelationBookRecPath);
		FileOutputFormat.setOutputPath(job, new Path(listenCorelationBookRecPath));		
		
		log.info("job input path: " + listenFilteredIndicatorPath);
		log.info("job output path: " + listenCorelationBookRecPath);		
		
		job.setMapperClass(ListenCorelationBookRecMapper.class);
		job.setReducerClass(ListenCorelationBookRecReducer.class);
		job.setNumReduceTasks(config.getParam("listen_rec_reduce_task_num", 200));
		
				
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		

		
		return 0;
	}
	
	
	public static void main( String[] args ) throws Exception { 	
    	PluginUtil.getInstance().init(args);	
    	Logger log = PluginUtil.getInstance().getLogger();
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new ListenCorelationRecDriver(), args);	

		Date dateEnd = new Date();


		long timeCost = dateEnd.getTime() - dateBeg.getTime();		

		PluginResult result = PluginUtil.getInstance().getResult();		
		result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();
		
		log.info("time cost in total(ms) :" + timeCost) ;
		System.exit(ret);	
    }
	
	
	public void check(String fileName) {
		Logger log = PluginUtil.getInstance().getLogger();
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
	
	 public static class KeyPartition extends Partitioner<TextPair, TextPair>{
			@Override
			public int getPartition(TextPair key, TextPair value, int numPartitions) {
				return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
			}    	
	    }
		

}
