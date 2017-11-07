package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;
import com.eb.bi.rs.frame.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class BookScroeDriver extends Configured  implements Tool{
	private static PluginUtil pluginUtil;
	private static Logger log;
	
	
	public BookScroeDriver(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Date dateBeg = new Date();
				
		int ret = ToolRunner.run(new Configuration(), new BookScroeDriver(args), args);
				
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
		
		log.info("time cost in total(ms) :" + timeCost);
				
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		Job job = null;
		long start;
		//配置加载=============================================================================
		int reduceNum1 = config.getParam("reduce_num_job1", 1);
		
		int maxSplitSizejob1 = config.getParam("tagdict_max_split_size_job1", 64);
		
//		String hdfsworkdir = config.getParam("hdfs_work_path", "");
		
//		String inputfiledir = config.getParam("input_file_path", "");
//		String outputfiledir = config.getParam("output_file_path", "");
		
//		String hadoopIp = config.getParam("hadoop_ip", "");
		
		String inputhdfsdir1 = config.getParam("intput_hdfs_path1", "");
		String inputhdfsdir2 = config.getParam("intput_hdfs_path2", "");
		String inputhdfsdir3 = config.getParam("intput_hdfs_path3", "");
		
		String workhdfspath = config.getParam("work_hdfs_path", "");
		
//		String outputhdfsdir = config.getParam("outtput_hdfs_path", "");
		
		String k_v_separator = config.getParam("k_v_separator", "");
		
		String charge_type1 = config.getParam("charge_type1", "2");
		String charge_type2 = config.getParam("charge_type2", "1");
		String charge_type3 = config.getParam("charge_type3", "0");
		
		//数据准备：导入HDFS=====================================================================
//		PutMergeToHdfs.put(inputfiledir, hadoopIp, inputhdfsdir1);
		//==================================================================================
		start = System.currentTimeMillis();
		//==================================================================================
		conf = new Configuration(getConf());
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob1));
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
		conf.set("mapred.textoutputformat.separator",k_v_separator);
		
		conf.set("charge_type1",charge_type1);
		conf.set("charge_type2",charge_type2);
		conf.set("charge_type3",charge_type3);
		
		check(workhdfspath,conf);
		
		//计算图书阅读用户数
		job = new Job(conf);
		job.setJarByClass(BookScroeDriver.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputhdfsdir3));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(workhdfspath + "/bookreadnum"));
		
		//设置M-R
		job.setMapperClass(CountReadnumMapper.class);
		job.setNumReduceTasks(reduceNum1);
		job.setReducerClass(CountReadnumReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");
		
		//图书打分TYPE划分
		job = new Job(conf);
		job.setJarByClass(BookScroeDriver.class);
		
		MultipleInputs.addInputPath(job, new Path(inputhdfsdir2), KeyValueTextInputFormat.class, BookChargetypeMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputhdfsdir1), KeyValueTextInputFormat.class, BookTotalChMapper.class);//workhdfspath + "/bookCHnum/part-*"	
		
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(workhdfspath + "/bookTYPE"));
		
		job.setNumReduceTasks(1);
		job.setReducerClass(BookTypeReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
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
		
		
		
		//用户图书主体得分计算
		//-----
		//添加内存加载图书|type
		log.info("mapred.cache.localFiles No.1"+conf.getStrings("mapred.cache.localFiles"));
		log.info("mapred.cache.localFiles No.1"+conf.getStrings("mapred.cache.files"));
				
		FileSystem fs1 = FileSystem.get(conf);	
		FileStatus[] status1 = fs1.globStatus(new Path(workhdfspath + "/bookTYPE/part-*"));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf);	
					
			//DistributedCache.addLocalFiles(conf,status1[i].getPath().toString());
					
			log.info("book_type file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
				
		log.info("mapred.cache.localFiles No.2"+conf.getStrings("mapred.cache.localFiles"));
		log.info("mapred.cache.localFiles No.2"+conf.getStrings("mapred.cache.files"));
				
		FileStatus[] status2 = fs1.globStatus(new Path(workhdfspath + "/bookreadnum/part-*"));
		for(int i = 0;  i  < status2.length; i++){
			DistributedCache.addCacheFile(URI.create(status2[i].getPath().toString()),conf);	
					
			//DistributedCache.addLocalFiles(conf,status2[i].getPath().toString());
					
			log.info("book_readnum file: " + status2[i].getPath().toString() + " has been add into distributed cache");
		}
				
		log.info("mapred.cache.localFiles No.3"+conf.getStrings("mapred.cache.localFiles"));
		log.info("mapred.cache.localFiles No.3"+conf.getStrings("mapred.cache.files"));
		//-----
	
		job = new Job(conf);
		job.setJarByClass(BookScroeDriver.class);
		
		//MultipleInputs.addInputPath(job, new Path(inputhdfsdir3), TextInputFormat.class, UserHisMapper.class);
		//MultipleInputs.addInputPath(job, new Path(workhdfspath + "/bookTYPE/part-*"), KeyValueTextInputFormat.class, BooktypeInputMapper.class);
		
		//设置Map
		FileInputFormat.setInputPaths(job, new Path(inputhdfsdir3));
		
		//job.setMapperClass(UserHisMapper.class);
		job.setMapperClass(NewUserhisMapper.class);
		
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(workhdfspath + "/userbookMpoint"));
		
		job.setNumReduceTasks(0);
		//job.setReducerClass(NewUserhisReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
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
		
		
		
		//用户图书附加得分计算
		job = new Job(conf);
		job.setJarByClass(BookScroeDriver.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(workhdfspath + "/userbookMpoint/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(workhdfspath + "/userbookscore"));
		
		//设置M-R
		job.setMapperClass(UserBookSpointMapper.class);
		job.setNumReduceTasks(reduceNum1);
		job.setReducerClass(UserBookSpointReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
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
