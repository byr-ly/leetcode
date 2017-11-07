package com.eb.bi.rs.frame.recframe.resultcal.offline.supplementer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class SupplementDriver extends BaseDriver{
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("SupplementDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		String inputPath1 = app_conf.getProperty("hdfs.input.path.1");//待补白数据
		//String dataformatType1 = app_conf.getProperty("Appconf.data.input.format.type1");
		String inputPath2 = app_conf.getProperty("hdfs.input.path.2");//黑名单数据
		String dataformatType2 = app_conf.getProperty("Appconf.data.input.format.type2");
		
		String cachePath = app_conf.getProperty("hdfs.input.fillter.db.path");//补白库
		
		String workPath = app_conf.getProperty("hdfs.work.path");
		
		String outPath = app_conf.getProperty("hdfs.output.path.1");//推荐结果
		
		String in_ifhaveSuffix = app_conf.getProperty("Appconf.result.recommend.in.ifhaveSuffix");
		String out_ifhaveSuffix = app_conf.getProperty("Appconf.result.recommend.out.ifhaveSuffix");
		
		String inputresultformatType = app_conf.getProperty("Appconf.result.recommend.inputresultformatType");
		String outputdataformatType = app_conf.getProperty("Appconf.result.recommend.outputdataformatType");
		
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		String k_v_separator = app_conf.getProperty("hadoop.io.k_v_separator");
		
		//String dataformatType = app_conf.getProperty("Appconf.data.format.type");
		String recommendMinum = app_conf.getProperty("Appconf.result.recommend.minnum");
		//--------------------------------------------------------
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
		conf.set("mapred.textoutputformat.separator",k_v_separator);
		
		//conf.set("Appconf.data.input.format.type1",dataformatType1);
		conf.set("Appconf.data.input.format.type2",dataformatType2);
		
		conf.set("Appconf.result.recommend.minnum",recommendMinum);
		
		conf.set("Appconf.result.recommend.in.ifhaveSuffix",in_ifhaveSuffix);
		conf.set("Appconf.result.recommend.out.ifhaveSuffix",out_ifhaveSuffix);
		conf.set("Appconf.result.recommend.inputresultformatType",inputresultformatType);
		conf.set("Appconf.result.recommend.outputdataformatType",outputdataformatType);
		//--------------------------------------------------------
		//补白库加载
		FileSystem fs1 = FileSystem.get(conf);	
		FileStatus[] status1 = fs1.globStatus(new Path(cachePath));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf);	
			log.info("book_fillter_data file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
		
		job = new Job(conf);
		job.setJarByClass(SupplementDriver.class);
		
		//待补白结果
		MultipleInputs.addInputPath(job, new Path(inputPath1), KeyValueTextInputFormat.class, Supplement1ToberecResultMapper.class);
		//黑名单
		String[] inputPaths = inputPath2.split(";");
		
		if(!inputPath2.equals("")){
			for(int i = 0;i != inputPaths.length;i++){
				MultipleInputs.addInputPath(job, new Path(inputPaths[i]), KeyValueTextInputFormat.class, Supplement2BlackListMapper.class);
			}
		}
		check(outPath,conf);
		
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(SupplementReducer.class);
		
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
