package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.algorithm.occurrence.CooccurrenceMatrixManager;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class RelatedKeyOfflineDriver extends Configured  implements Tool{
	private static PluginUtil pluginUtil;
	private static Logger log;
	private static Configuration conf;
	/**
	 * @param args
	 * 
	 */
	
	public RelatedKeyOfflineDriver(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new Configuration(), new RelatedKeyOfflineDriver(args), args);
		
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

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		/* 计算流程
		 * 1.分词提取有结果词的关键词，得到过滤依据表
		 * 2.对全部搜索结果分词，保留有结果关键词记录，得到共现矩阵输入数据
		 * 3.共现矩阵模块调用，得到热词表，共现矩阵
		 * 4.共现矩阵减维，过滤掉热词
		 * 5.输出共现矩阵topN
		 * */
		
		PluginConfig config = pluginUtil.getConfig();
		
		long start;
		//任务配置加载============================================================================
		int reduceNum = config.getParam("reduce_num_job", 1);
    	
    	int mapSize1 = config.getParam("tagdict_max_split_size_job1", 64);
    	int mapSize2 = config.getParam("tagdict_max_split_size_job2", 64);
    	int mapSize3 = config.getParam("tagdict_max_split_size_job3", 64);
    	int mapSize4 = config.getParam("tagdict_max_split_size_job3", 64);
    	int mapSize5 = config.getParam("tagdict_max_split_size_job3", 64);
    	int mapSize6 = config.getParam("tagdict_max_split_size_job3", 64);	
    	//====================================================================================
    	String inputfiledir1  = config.getParam("input_file_haveresult_path", "");
    	String inputfiledir2  = config.getParam("input_file_all_path", "");
    	
    	String inputfiledir3  = config.getParam("input_file_bookauthorname_path", "");
    	
    	String inputhdfsdir1 = config.getParam("input_hdfs_haveresult_path", "");
    	String inputhdfsdir2 = config.getParam("input_hdfs_all_path", "");
    	
    	String inputhdfsdir3 = config.getParam("input_hdfs_bookauthorname_path", "");
    
    	//====================================================================================
//    	String outputfiledir = config.getParam("output_file_path", "");
    	
    	String hdfsworkdir = config.getParam("hdfs_work_path", "");
    	//====================================================================================
    	String usefulwordlistdir  = config.getParam("Hdfs_usefulword", "");
    	String userkeyworddatadir = config.getParam("Hdfs_userkeyworddata", "");
    	String cooccurrencematrixdir = config.getParam("Hdfs_matrix_path", "");
    	
    	String cooccurrencematrixallresultdir  = config.getParam("Hdfs_matrixtopN_input", "");
    	String cooccurrencematrixtopNresultdir = config.getParam("Hdfs_matrixtopN_output", "");
    	
    	String hotwordlistdir = config.getParam("Hdfs_hotwordlist_output", "");
    	
    	//====================================================================================
//    	String hadoopIp = config.getParam("hadoop_ip", "");		
		//====================================================================================
		start = System.currentTimeMillis();
		//hadoop配置加载=========================================================================
		conf = new Configuration(getConf());
		
		Date worktime = new Date(); 
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		String record_day       = config.getParam("record_day", sdf.format(worktime));
		
		String k_v_separator    = config.getParam("k_v_separator", "|");
    	int neighbourNum        = config.getParam("neighbour_num", 0);
    	String id_id_separator  = config.getParam("id_id_separator", ",");
    	String id_num_separator = config.getParam("id_num_separator", ":");
    	String top_num          = config.getParam("top_num", "4");
    	String hot_top_num      = config.getParam("hot_top_num", "20");
    	//-------------------------------------------------------
    	conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
    	conf.set("mapred.textoutputformat.separator",k_v_separator);
    	//-------------------------------------------------------
    	conf.set("record_day", record_day);
    	
    	conf.set("neighbour_num", String.valueOf(neighbourNum));
    	conf.set("id_id_separator", id_id_separator);
    	conf.set("id_num_separator", id_num_separator);
    	conf.set("top_num", top_num);
    	conf.set("hot_top_num", hot_top_num);
    	//====================================================================================
    	rmr(hdfsworkdir,conf);
    	//====================================================================================
//		PutMergeToHdfs.put(inputfiledir1, hadoopIp, inputhdfsdir1);//有结果的用户搜索记录
//		PutMergeToHdfs.put(inputfiledir2, hadoopIp, inputhdfsdir2);//全部的用户搜索记录
		
//		PutMergeToHdfs.put(inputfiledir3, hadoopIp, inputhdfsdir3 + "/book_and_author");//作者名称表,图书名称表
		//====================================================================================
		//1.分词提取有结果词的关键词，得到过滤依据表
		Configuration conf1 = conf;
		conf1.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize1));
		
		FileSystem fs1 = FileSystem.get(conf1);	
		FileStatus[] status1 = fs1.listStatus(new Path(inputhdfsdir3));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf1);	
			log.info("book_and_author_name file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
		
		Job job1 = new Job(conf1);
		job1.setJarByClass(RelatedKeyOfflineDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job1, new Path(inputhdfsdir1));
		//设置输出地址
		FileOutputFormat.setOutputPath(job1, new Path(usefulwordlistdir));
		//设置M-R
		job1.setMapperClass(AnalyzeWordMapper.class);
		job1.setNumReduceTasks(reduceNum);
		job1.setReducerClass(AnalyzeWordReducer.class);
		//设置输入/输出格式
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		//日志==================================================================================
		if( job1.waitForCompletion(true)){
			log.info("job[" + job1.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job1.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");	
		//====================================================================================
		//2.对全部搜索结果分词，保留有结果关键词记录，得到共现矩阵输入数据
		Configuration conf2 = conf;
		conf2.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize2));
		
		FileSystem fs2 = FileSystem.get(conf2);	
		FileStatus[] status2 = fs2.listStatus(new Path(inputhdfsdir3));
		for(int i = 0;  i  < status2.length; i++){
			DistributedCache.addCacheFile(URI.create(status2[i].getPath().toString()),conf2);	
			log.info("book_and_author_name file: " + status2[i].getPath().toString() + " has been add into distributed cache");
		}
		
		Job job2 = new Job(conf2);
		job2.setJarByClass(RelatedKeyOfflineDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job2, new Path(inputhdfsdir2));
		//设置输出地址
		FileOutputFormat.setOutputPath(job2, new Path(userkeyworddatadir));
		//设置M-R
		job2.setMapperClass(KeywordGenerateMapper.class);
		job2.setNumReduceTasks(reduceNum);
		job2.setReducerClass(KeywordGenerateReducer.class);
		//设置输入/输出格式
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job2.waitForCompletion(true)){
			log.info("job[" + job2.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job2.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");	
		//====================================================================================
		//3.共现矩阵模块调用，得到热词表，共现矩阵
		if(func1(arg0,userkeyworddatadir + "/part-*",cooccurrencematrixdir,reduceNum,mapSize3) != 0){
			return 1;
		}
		//====================================================================================
		//4.共现矩阵减维，过滤掉热词
		Configuration conf4 = conf;
		conf4.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize4));
		FileSystem fs4 = FileSystem.get(conf4);
		
		FileStatus[] status4 = fs4.globStatus(new Path(usefulwordlistdir + "/part-*"));
		for(int i = 0;  i  < status4.length; i++){
			DistributedCache.addCacheFile(URI.create(status4[i].getPath().toString()),conf4);	
			log.info("cooccurrence file: " + status4[i].getPath().toString() + " has been add into distributed cache");
		}
		
		FileStatus[] status5 = fs4.globStatus(new Path(cooccurrencematrixdir + "/frequency/part-*"));
		for(int i = 0;  i  < status5.length; i++){
			DistributedCache.addCacheFile(URI.create(status5[i].getPath().toString()),conf4);	
			log.info("cooccurrence file: " + status5[i].getPath().toString() + " has been add into distributed cache");
		}
		
		Job job4 = new Job(conf4);
		job4.setJarByClass(RelatedKeyOfflineDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job4, new Path(cooccurrencematrixdir + "/cooccurrence/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job4, new Path(cooccurrencematrixallresultdir));
		//设置M-R
		job4.setMapperClass(FilterHotwordMapper.class);
		job4.setNumReduceTasks(0);
		//设置输入/输出格式
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job4.waitForCompletion(true)){
			log.info("job[" + job4.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job4.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");	

		//====================================================================================
		//5.输出共现矩阵topN
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize5));
		Job job5 = new Job(conf);
		job5.setJarByClass(RelatedKeyOfflineDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job5, new Path(cooccurrencematrixallresultdir + "/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job5, new Path(cooccurrencematrixtopNresultdir));
		//设置M-R
		job5.setMapperClass(OutputTopNMapper.class);
		job5.setNumReduceTasks(reduceNum);
		job5.setReducerClass(OutputTopNReducer.class);
		//设置输入/输出格式
		job5.setInputFormatClass(KeyValueTextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job5.waitForCompletion(true)){
			log.info("job[" + job5.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job5.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");		
		//====================================================================================
		//6.输出热词表
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize6));
		Job job6 = new Job(conf);
		job6.setJarByClass(RelatedKeyOfflineDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job6, new Path(cooccurrencematrixdir + "/frequency/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job6, new Path(hotwordlistdir));
		//设置M-R
		job6.setMapperClass(HotWordListMapper.class);
		job6.setNumReduceTasks(1);
		job6.setReducerClass(HotWordListReducer.class);
		//设置输入/输出格式
		job6.setInputFormatClass(KeyValueTextInputFormat.class);
		job6.setOutputFormatClass(TextOutputFormat.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job6.waitForCompletion(true)){
			log.info("job[" + job6.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job6.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");		
		//====================================================================================		
		
		
		return 0;
	}

	public int func1(String[] args, String inputdir, String workpath, int reduceNum, int mapSize) throws Exception{
		int ret = 0;
		
		String outputdir1 = workpath + "/cooccurrence";
		String outputdir2 = workpath + "/frequency";
		String middledir = workpath + "/middle";
		
		ret = ret + CooccurrenceMatrixManager.Vertical2horizontal(args, log, conf, inputdir, middledir, reduceNum, mapSize);
    	ret = ret + CooccurrenceMatrixManager.Cooccurrence(args, log, conf, middledir, outputdir1, reduceNum, mapSize);
    	ret = ret + CooccurrenceMatrixManager.Frequency(args, log, conf, inputdir, outputdir2, reduceNum, mapSize);
		
		return ret;
	}
	
	public void rmr(String folder, Configuration conf) throws IOException 
	{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(path);
        log.info("Delete: " + folder);
        fs.close();
    }
	
}
