package com.eb.bi.rs.mras2.cartoonrec.corelationrec;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.selfSign.SelfSignedRecMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.selfSign.SelfSignedRecReducer;

public class SelfSignedRecDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Logger log = PluginUtil.getInstance().getLogger();
		PluginConfig config = PluginUtil.getInstance().getConfig();		
		
		Configuration conf ;
		Job job;
		FileStatus[] status;
		long start;
				
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：推荐结果数据
		 **     2.缓存文件：图书分类信息
		 ** 输出：
		 **		将自签书提前到前十位的推荐结果数据
		 ** 功能描述：    
		 ** 	1.筛选规则
		 **       推荐结果50本结果中前10本中要有N本自签书推荐库中的图书，暂定N=4
		 **     2.补白策略
		 **       先在原来推荐结果里筛选，若不够N,则去classid里补白自签书,若还不够N,则去big_class
		 **       里补够N,若还不够N，则自签书数量就为补白出来的数量
		 **
		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	

		String recResultInputPath = config.getParam("read_corelation_book_recommend_selfsign_path","");
		String cacheInputPath = config.getParam("bookinfo_cache_input","");
		String recResultOutputPath = config.getParam("read_corelation_book_recommend_path", "");
		
		int reduceNum = config.getParam("reduce_num", 100);
		int selfIndex = config.getParam("self_index", 5);
		
		conf = new Configuration(getConf());
		conf.set("cache.input.path", cacheInputPath);
		conf.setInt("self.index", selfIndex);
		
		job = new Job(conf, "CorelationRecommend-ReadAlsoRead-read_selfsign_book_recommend");

		status = FileSystem.get(conf).globStatus(new Path(cacheInputPath + "/*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("cache data input file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
				
		job.setJarByClass(getClass());
			
		check(recResultOutputPath);
		
		FileInputFormat.setInputPaths(job, new Path(recResultInputPath ));
		FileOutputFormat.setOutputPath(job, new Path(recResultOutputPath));		
			
		log.info("job input path: " + recResultInputPath);
		log.info("job output path: " + recResultOutputPath);	
		
		job.setMapperClass(SelfSignedRecMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(reduceNum);
		
		job.setReducerClass(SelfSignedRecReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		
		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	

		String recResultInputPath2 = config.getParam("read_self_recresult_for_book_without_cooccurrenceinfo_path","");
		String recResultOutputPath2 = config.getParam("read_recresult_for_book_without_cooccurrenceinfo_path", "");
		job = new Job(conf, "CorelationRecommend-ReadAlsoRead-read_selfsign_book_recommend_without_cooccurrenceinfo");

		status = FileSystem.get(conf).globStatus(new Path(cacheInputPath + "/*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("cache data input file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
				
		job.setJarByClass(getClass());
			
		check(recResultOutputPath2);
		
		FileInputFormat.setInputPaths(job, new Path(recResultInputPath2 ));
		FileOutputFormat.setOutputPath(job, new Path(recResultOutputPath2));		
			
		log.info("job input path: " + recResultInputPath2);
		log.info("job output path: " + recResultOutputPath2);	
		
		job.setMapperClass(SelfSignedRecMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(reduceNum);
		
		job.setReducerClass(SelfSignedRecReducer.class);
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
		
		int ret = ToolRunner.run(new SelfSignedRecDriver(), args);	

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
}
