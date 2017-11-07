package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class UserAppPrefDriver extends Configured implements Tool
{	
	private static PluginUtil pluginUtil;
	private static Logger log;
	private static PluginConfig config;	
	public UserAppPrefDriver(String[] args)
	{
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		
		config     = pluginUtil.getConfig();
		log        = pluginUtil.getLogger();
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{		

		PluginConfig config = pluginUtil.getConfig();
		String indicatorCount = config.getParam("indicatorcount", "");
		
		
		log.info("indicator count = " + indicatorCount);

		/*job 1:�����ָ��ȫ�ֵ����ֵ/��Сֵ  */
		/*output data format
		 *         1116.0:1.0:1.0:1.0:20838.69:0.21:21252.88:0.01
		 */
		log.info("job start to compute statitics for global data");
		long start = System.currentTimeMillis();
		Configuration conf1 = new Configuration();
		conf1.set("indicatorCount", indicatorCount);
		
		Job job1 = new Job(conf1, "compute statitics for global data");
		//log.info("job[" + job1.getJobID() + "] start to compute statitics for global data");
		job1.setJarByClass(UserAppPrefDriver.class);		
		
		String input = config.getParam("globaldatadir", "");
		String output = config.getParam("globalstatiticsdir", "");		
		log.info("job input path: " + input);
		log.info("job output path: " + output);
		check(output);
		FileInputFormat.setInputPaths(job1, new Path(input));		
		FileOutputFormat.setOutputPath(job1, new Path(output));
		
		job1.setMapperClass(MaxMinMapper.class);
		job1.setCombinerClass(MaxMinReducer.class);
		job1.setReducerClass(MaxMinReducer.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		if( job1.waitForCompletion(true)){
			log.info("job[" + job1.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job1.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");
		
		/*job 2: ����job 1�ļ�������б�׼���Ĵ���  */
		log.info("job start to normalize global data ");
		start = System.currentTimeMillis();
		Configuration conf2 = new Configuration();
		conf2.set("indicatorCount", indicatorCount);	
		String defaultName = conf2.get("fs.default.name");		 
		if(defaultName.endsWith("/")){
			defaultName = defaultName.substring(0,defaultName.length() -1);
		}
		String filedir = config.getParam("globalstatiticsdir", "");
		if(filedir.endsWith("/")){
			filedir = filedir.substring(0, filedir.length() - 1);
		}
		String uri = defaultName + filedir + "/part-r-00000";
		DistributedCache.addCacheFile(new URI(uri), conf2); 
		log.info(uri + " has been add to cache");
		
		Job job2 = new Job(conf2, "normaliz global data ");
		job2.setJarByClass(UserAppPrefDriver.class);
		
		input =  config.getParam("globaldatadir", "");
		output = config.getParam("normalizeddatadir", "");
		check(output);
		FileInputFormat.setInputPaths(job2, new Path(input));
		FileOutputFormat.setOutputPath(job2, new Path(output));
		log.info("job input path: " + input);
		log.info("job output path: " + output);
		
		job2.setMapperClass(NormalizeMapper.class);
		job2.setNumReduceTasks(0);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		if( job2.waitForCompletion(true))
		{
			log.info("job[" + job2.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else 
		{
			log.error("job[" + job2.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");
		
		/*job 3: ����ҵ�����ͼ����ָ������ֵ����Сֵ���͡���¼�� 
		 * output file data format:
		 * 0       maxIndicators:1.0:1.0:1.0:1.0
		 * 0       minIndicators:0.0:1.0:0.0:0.0(��ʵû��)
		 * 0       sumIndicators:27.91121076233164:1930.0:19.57750757252926:16.036420022331104
		 * 0       rcdCount:1931
		 */
		log.info("job start to compute statitics for normalized data group by service type");
		start = System.currentTimeMillis();
		Configuration conf3 = new Configuration();
		conf3.set("indicatorCount", indicatorCount);
		//conf3.set("mapred.job.tracker", "10.1.1.240:9001");		

		Job job3 = new Job(conf3, "compute statitics for normalized data group by service type");
		//log.info("job[" + job3.getJobID() + "] start to compute statitics for normalized data group by service type");
		job3.setJarByClass(UserAppPrefDriver.class);	
		
		input = config.getParam("normalizeddatadir", "");
		if(input.endsWith("/")){
			input = input.substring(0, input.length() - 1);			
		}
		input += "/part-m-*";
		output = config.getParam("servicetypestatiticsdir", "");		
		check(output);
		FileInputFormat.setInputPaths(job3, new Path(input));
		FileOutputFormat.setOutputPath(job3, new Path(output));
		log.info("job input path: " + input);
		log.info("job output path: " + output);
		
		job3.setMapperClass(ServiceTypeMapper.class);
		job3.setCombinerClass(ServiceTypeReducer.class);
		job3.setReducerClass(ServiceTypeReducer.class);
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		if( job3.waitForCompletion(true)){
			log.info("job[" + job3.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job3.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");

		
		
		/*job 4: */
		/*output data format
		 * 0       0.1324986721088442:0.13220806479504482:0.13298488602776443:0.13263696419899132
		 * 0       0.1324986721088442:0.13220806479504482:0.13298488602776443:0.13263696419899132
		 */
		log.info("job start to compute indicator entropy ");
		start = System.currentTimeMillis();
		Configuration conf4 = new Configuration();
		conf4.set("indicatorCount", indicatorCount);
		//conf4.set("mapred.job.tracker", "10.1.1.240:9001");		
		defaultName = conf4.get("fs.default.name");		 
		if(defaultName.endsWith("/")){
			defaultName = defaultName.substring(0,defaultName.length() -1);
		}
		filedir = config.getParam("servicetypestatiticsdir", "");
		if(filedir.endsWith("/")){
			filedir = filedir.substring(0, filedir.length() - 1);
		}
		uri = defaultName + filedir + "/part-r-00000";
		DistributedCache.addCacheFile(new URI(uri), conf4); 
		log.info(uri + " has been add to cachefile");		
		
		Job job4 = new Job(conf4, "compute indicator entropy");	
		job4.setJarByClass(UserAppPrefDriver.class);		
		
		input = config.getParam("normalizeddatadir", "");
		if(input.endsWith("/")){
			input = input.substring(0, input.length() - 1);			
		}
		input += "/part-m-*";
		output = config.getParam("indicatorentropy", "");		
		check(output);
		FileInputFormat.setInputPaths(job4, new Path(input));
		FileOutputFormat.setOutputPath(job4, new Path(output));
		log.info("job input path: " + input);
		log.info("job output path: " + output);

		
		job4.setMapperClass(EntropyMapper.class);
		job4.setCombinerClass(EntropyReducer.class);
		job4.setReducerClass(EntropyReducer.class);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		if( job4.waitForCompletion(true)){
			log.info("job[" + job4.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job4.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}	

		log.info("=================================================================================");	
		
		
		/*job 5:*/
		log.info("job start to compute user appplication preference");
		start = System.currentTimeMillis();
		Configuration conf5 = new Configuration();
		conf5.set("indicatorCount", indicatorCount);
		//conf5.set("mapred.job.tracker", "10.1.1.240:9001");	

		defaultName = conf5.get("fs.default.name");		 
		if(defaultName.endsWith("/")){
			defaultName = defaultName.substring(0,defaultName.length() -1);
		}
		filedir = config.getParam("servicetypestatiticsdir", "");
		if(filedir.endsWith("/")){
			filedir = filedir.substring(0, filedir.length() - 1);
		}
		uri = defaultName + filedir + "/part-r-00000";
		DistributedCache.addCacheFile(new URI(uri), conf5); 
		log.info(uri + " has been add to cachefile");
		
		filedir = config.getParam("indicatorentropy", "");
		if(filedir.endsWith("/")){
			filedir = filedir.substring(0, filedir.length() - 1);
		}
		uri = defaultName + filedir + "/part-r-00000";
		DistributedCache.addCacheFile(new URI(uri), conf5); 
		log.info(uri + " has been add to cachefile");
		
		Job job5 = new Job(conf5, "compute user appplication preference");	
		job5.setJarByClass(UserAppPrefDriver.class);	
		
		input= config.getParam("normalizeddatadir", "");
		if(input.endsWith("/")){
			input = input.substring(0, input.length() - 1);			
		}
		input += "/part-m-*";
		output = config.getParam("finalresult", "");		
		check(output);
		FileInputFormat.setInputPaths(job5, new Path(input));
		FileOutputFormat.setOutputPath(job5, new Path(output));
		log.info("job input path: " + input);
		log.info("job output path: " + output);

		
		job5.setMapperClass(UserAppPrefMapper.class);
		job5.setNumReduceTasks(0);
		
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		
		if( job5.waitForCompletion(true)){
			log.info("job[" + job5.getJobName() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job5.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		return 0;
	}
	/*
	public static void main(String[] args) throws Exception {		

		Date dateBegin = new Date();
		//run the jobs
		int ret = ToolRunner.run(new Configuration(), new UserAppPrefDriver(args), args);	

		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(dateEnd);		
		long timeCost = dateEnd.getTime() - dateBegin.getTime();		

		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();
		
		log.info("time cost in total(ms) :" + timeCost) ;
		System.exit(ret);	
	}	
	*/
	public void check(String fileName) 
	{
		try 
		{
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


