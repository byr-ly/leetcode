package com.eb.bi.rs.mras.voicebookrec.lastpage;

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

import com.eb.bi.rs.frame.common.hadoop.fileopt.GetMergeFromHdfs;
import com.eb.bi.rs.frame.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;



public class LastPageDriver extends Configured implements Tool {
	private static PluginUtil pluginUtil;
	private static Logger log;
	public LastPageDriver(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}



	public static void main(String[] args) throws Exception {

		
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new LastPageDriver(args), args);	

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
		Configuration conf ;
		Job job;
		long start;	
		
		
		String localBookInfoPath = config.getParam("local_book_info_path", "");
		String hdfsBookInfoPath = config.getParam("hdfs_book_info_path","");
		PutMergeToHdfs.put(localBookInfoPath,  hdfsBookInfoPath);
		


		/*=============================订购还订购======================================*/
		log.info("======================ORDER ALSO ORDER===========================");

		log.info("=================================================================");		
		/*将图书订购数与图书信息表进行表连接，输出栏目图书订购数表*/
		log.info("job start to Join book frequency whith column ids");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");		
		job = new Job(conf,"Join book frequency whith column ids");
		
		job.setJarByClass(getClass());
	
		String orderBookFrequencyPath = config.getParam("order_book_frequency_path", "");
		String orderColumnbookFrequencyPath = config.getParam("order_column_book_frequency_path", "");	
		log.info("job input path: " + hdfsBookInfoPath);
		log.info("job input path: " + orderBookFrequencyPath);		
		log.info("job output path: " + orderColumnbookFrequencyPath);
		
		MultipleInputs.addInputPath(job, new Path(hdfsBookInfoPath), TextInputFormat.class, JoinColumnMapper.class);		
		MultipleInputs.addInputPath(job, new Path(orderBookFrequencyPath+"/part-*"), TextInputFormat.class, JoinFrequencyMapper.class);
		check(orderColumnbookFrequencyPath);
		FileOutputFormat.setOutputPath(job, new Path(orderColumnbookFrequencyPath));
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);		
		
		job.setReducerClass(JoinColumnFrequencyReducer.class);
		job.setOutputKeyClass(Text.class);	
		job.setNumReduceTasks(config.getParam("order_job1_reduce_number", 6));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			//return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		log.info("=====================================================");	
		/*将图书信息与订购共现矩阵左外连接*/
		log.info("job start to join bookid with order cooccurrence matrix");	
		start = System.currentTimeMillis();				
		conf = new Configuration(getConf());		
		job = new Job(conf,"join bookid with order cooccurrence matrix");
		job.setJarByClass(getClass());		
		
		String orderBookCooccurrencePath = config.getParam("order_book_cooccurrence_path", "");
		String OrderJoinBookCooccurrencePath = config.getParam("order_join_book_cooccurrence_path", "");
		
		MultipleInputs.addInputPath(job, new Path(hdfsBookInfoPath), TextInputFormat.class, JoinBookMapper.class);		
		MultipleInputs.addInputPath(job, new Path(orderBookCooccurrencePath + "/part-r*"), TextInputFormat.class, JoinCooccurrenceMapper.class);
		check(OrderJoinBookCooccurrencePath);
		FileOutputFormat.setOutputPath(job, new Path(OrderJoinBookCooccurrencePath));
		log.info("job input path: " + hdfsBookInfoPath);
		log.info("job input path: " + orderBookCooccurrencePath);		
		log.info("job output path: " + OrderJoinBookCooccurrencePath);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setReducerClass(JoinBookCoocurrenceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("order_job2_reduce_number", 6));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			//return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		
		log.info("=====================================================");	
		
		
		
		/*计算订购还订购推荐结果*/
		log.info("job start to compute order also order result");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());

		DistributedCache.addCacheFile(URI.create(hdfsBookInfoPath), conf);
		log.info("book info file: " + hdfsBookInfoPath + " has been add into distributed cache");
		conf.set("book.info.path", hdfsBookInfoPath);
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(orderColumnbookFrequencyPath + "/part-*"));		
		for(int i = 0; i < status.length; i++){
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	/*如何在读取的时候区别，根据字段数目吗*/
			log.info("special tag file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}	
		
		conf.set("order.column.book.frequency.path", orderColumnbookFrequencyPath);	
		conf.setInt("order.recommend.number", config.getParam("order_recommend_number", 10));
		conf.setInt("order.pow.number", config.getParam("order_pow_number", 2));
		conf.setInt("order.frequency.threshold", config.getParam("order_frequency_threshold", 5));
		conf.setFloat("order.ratio.adjust.factor", Float.parseFloat(config.getParam("order_ratio_adjust_factor", "1.0")));
		
		
		job = new Job(conf,"order also order");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(OrderJoinBookCooccurrencePath + "/part-*"));
		String orderAlsoOrderPath = config.getParam("order_also_order_path", "");
		check(orderAlsoOrderPath);		
		FileOutputFormat.setOutputPath(job, new Path(orderAlsoOrderPath));
		log.info("job input path: " + OrderJoinBookCooccurrencePath);
		log.info("job output path: " + orderAlsoOrderPath);
		
		
		job.setMapperClass(OrderAlsoOrderMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringDoublePair.class);
	
		job.setReducerClass(OrderAlsoOrderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("order_job3_reduce_number", 6));
		
	
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		GetMergeFromHdfs.get( orderAlsoOrderPath + "/part-*", config.getParam("local_order_also_order_path", ""));		
		
		/*阅读还阅读*/
		log.info("=======================READ ALSO READ============================");		
		/*计算出系列图书*/		
		log.info("=================================================================");		
		log.info("job start to compute serial book");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");
		conf.setInt("read.recommend.serial.book.number", config.getParam("read_recommend_serial_book_number", 2));
		
		job = new Job(conf,"compute serial book");
		job.setJarByClass(LastPageDriver.class);	
		
		FileInputFormat.addInputPath(job, new Path(hdfsBookInfoPath));		
		String readSerialBookPath = config.getParam("read_serial_book_path", "");
		check(readSerialBookPath);
		FileOutputFormat.setOutputPath(job, new Path(readSerialBookPath));
		log.info("job input path: " + hdfsBookInfoPath);		
		log.info("job output path: " + readSerialBookPath);
		
		job.setMapperClass(SerialBookRecMapper.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setReducerClass(SerialBookRecReducer.class);
		job.setOutputValueClass(Text.class);		
		job.setNumReduceTasks(config.getParam("read_job1_reduce_number", 6));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			//return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		
		/*将图书阅读数与图书信息表进行表连接，输出栏目图书阅读数表*/		
		log.info("=================================================================");		
		log.info("job start to Join book frequency whith column ids");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());			
		conf.set("mapred.textoutputformat.separator","|");		
		
		job = new Job(conf,"Join book frequency whith column ids");
		job.setJarByClass(LastPageDriver.class);
		
		String readBookFrequencyPath = config.getParam("read_book_frequency_path", "");/*#####确定是否一定是文件，应该是一个目录*/
		String readColumnbookFrequencyPath = config.getParam("read_column_book_frequency_path", "");	
		log.info("job input path: " + hdfsBookInfoPath);
		log.info("job input path: " + readBookFrequencyPath);		
		log.info("job output path: " + readColumnbookFrequencyPath);
		
		MultipleInputs.addInputPath(job, new Path(hdfsBookInfoPath), TextInputFormat.class, JoinColumnMapper.class);		
		MultipleInputs.addInputPath(job, new Path(readBookFrequencyPath + "/part-*"), TextInputFormat.class, JoinFrequencyMapper.class);
		check(readColumnbookFrequencyPath);
		FileOutputFormat.setOutputPath(job, new Path(readColumnbookFrequencyPath));
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);		
		
		job.setReducerClass(JoinColumnFrequencyReducer.class);
		job.setOutputKeyClass(Text.class);		
		job.setNumReduceTasks(config.getParam("read_job2_reduce_number", 6));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			//return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		
		log.info("=================================================================");
		/*将图书信息与阅读共现矩阵左外连接*/
		log.info("job start to join bookid with read cooccurrence matrix");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		job = new Job(conf,"join bookid with read cooccurrence matrix");
		job.setJarByClass(getClass());		
		
		String readBookCooccurrencePath = config.getParam("read_book_cooccurrence_path", "");/*####注意跟唐坤的接口，数据是否包含日志###########*/
		String readJoinBookCooccurrencePath = config.getParam("read_join_book_cooccurrence_path", "");
		
		MultipleInputs.addInputPath(job, new Path(hdfsBookInfoPath), TextInputFormat.class, JoinBookMapper.class);		
		MultipleInputs.addInputPath(job, new Path(readBookCooccurrencePath + "/part-*"), TextInputFormat.class, JoinCooccurrenceMapper.class);
		check(readJoinBookCooccurrencePath);
		FileOutputFormat.setOutputPath(job, new Path(readJoinBookCooccurrencePath));
		log.info("job input path: " + hdfsBookInfoPath);
		log.info("job input path: " + readBookCooccurrencePath);		
		log.info("job output path: " + readJoinBookCooccurrencePath);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setReducerClass(JoinBookCoocurrenceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("read_job3_reduce_number", 6));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			//return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}	
		
		
		
		log.info("=====================================================");
		/*计算阅读还阅读*/
		log.info("job start to compute read also read result");	
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		DistributedCache.addCacheFile(URI.create(hdfsBookInfoPath), conf);		
		log.info("book info file: " + hdfsBookInfoPath + " has been add into distributed cache");
		conf.set("book.info.path", hdfsBookInfoPath);

		
		/*栏目图书频次*/
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(readColumnbookFrequencyPath + "/part-*"));		
		for(int i = 0; i < status.length; i++){
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	/*如何在读取的时候区别，根据字段数目吗*/
			log.info("read column book frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}	
		conf.set("read.column.book.frequency.path", readColumnbookFrequencyPath);
		
		/*系列图书*/
		status = fs.globStatus(new Path( readSerialBookPath + "/part-*"));		
		for(int i = 0; i < status.length; i++){
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	/*如何在读取的时候区别，根据字段数目吗*/
			log.info("read serial book file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}
		conf.set("read.serial.book.path", readSerialBookPath);
		
		/*订购还订购结果*/
		status = fs.globStatus(new Path( orderAlsoOrderPath + "/part-*"));		
		for(int i = 0; i < status.length; i++){
			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()),conf);	/*如何在读取的时候区别，根据字段数目吗*/
			log.info("order also order file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}
		
		conf.set("order.also.order.path", orderAlsoOrderPath);
		conf.setInt("read.recommend.number",config.getParam("read_recommend_number", 10));
		conf.setInt("read.pow.number", config.getParam("read_pow_number", 2));
		conf.setInt("read.frequency.threshold", config.getParam("read_frequency_threshold", 10));
		conf.setFloat("read.ratio.adjust.factor", Float.parseFloat(config.getParam("read_ratio_adjust_factor", "1.0f")));
		
		
		job = new Job(conf,"read also read");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(readJoinBookCooccurrencePath + "/part-*"));
		String readAlsoReadPath = config.getParam("read_also_read_path", "");
		check(readAlsoReadPath);		
		FileOutputFormat.setOutputPath(job, new Path(readAlsoReadPath));
		log.info("job input path: " + readJoinBookCooccurrencePath);
		log.info("job output path: " + readAlsoReadPath);
		
		
		job.setMapperClass(ReadAlsoReadMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringDoublePair.class);
	
		job.setReducerClass(ReadAlsoReadReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("read_job4_reduce_number", 6));
	
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			GetMergeFromHdfs.get( readAlsoReadPath + "/part-*", config.getParam("local_read_also_read_path", ""));
			return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
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
	
	public static class KeyPartition extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)% numPartitions;
		}
	}

}
