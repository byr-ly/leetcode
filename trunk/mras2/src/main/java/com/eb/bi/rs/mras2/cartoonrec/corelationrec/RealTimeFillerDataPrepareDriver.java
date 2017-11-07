package com.eb.bi.rs.mras2.cartoonrec.corelationrec;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare.RealTimeFillerDataPrepareMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare.RealTimeFillerDataPrepareReducer;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare.RealTimeFillerFreeBookPrepareReducer;

public class RealTimeFillerDataPrepareDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Logger log = PluginUtil.getInstance().getLogger();
		PluginConfig config = PluginUtil.getInstance().getConfig();		
		
		Configuration conf ;
		Job job;
		long start;
		
		/***********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：阅读图书信息频次表
		 ** 输出：
		 **		分类热书
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		String bookAuthorClassFrequencyPath = config.getParam("read_class_frequency_path", "");
		String outputPath = config.getParam("read_class_hot_book_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("hot.book.count", config.getParam("hot_book_count", 200));		
	
		job = Job.getInstance(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		check(outputPath);	
		
		FileInputFormat.setInputPaths(job, new Path(bookAuthorClassFrequencyPath));	
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		log.info("job input path: " + bookAuthorClassFrequencyPath);
		log.info("job output path: " + outputPath);		
		
		job.setMapperClass(RealTimeFillerDataPrepareMapper.class);
		job.setReducerClass(RealTimeFillerDataPrepareReducer.class);
		job.setNumReduceTasks(1);	
		
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
		
		/***********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：订购图书信息频次表
		 ** 输出：
		 **		分类热书
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		bookAuthorClassFrequencyPath = config.getParam("order_class_frequency_path", "");
		outputPath = config.getParam("order_class_hot_book_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("hot.book.count", config.getParam("hot_book_count", 200));		
		
		job = Job.getInstance(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		check(outputPath);	
		
		FileInputFormat.setInputPaths(job, new Path(bookAuthorClassFrequencyPath));	
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		log.info("job input path: " + bookAuthorClassFrequencyPath);
		log.info("job output path: " + outputPath);		
		
		job.setMapperClass(RealTimeFillerDataPrepareMapper.class);
		job.setReducerClass(RealTimeFillerDataPrepareReducer.class);
		job.setNumReduceTasks(1);	
		
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
		/***********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书信息表
		 ** 输出：
		 **		作者书集表、全部书表、点击量表
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
	
		String bookinfoPath = config.getParam("book_info_path", "");
		
		conf = new Configuration(getConf());
		
		conf.set("author.book.output.path", config.getParam("author_book_path", ""));
		conf.set("all.book.output.path", config.getParam("all_book_path", ""));
		conf.set("book.click.output.path", config.getParam("book_click_path", ""));
		
		job = Job.getInstance(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
		
		outputPath = config.getParam("common_output_path", "");
		check(outputPath);	
		
		FileInputFormat.setInputPaths(job, new Path(bookinfoPath ));	
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		log.info("job input path: " + bookinfoPath);
		log.info("job output path: " + outputPath);		
		
		job.setMapperClass(RealTimeFillerDataPrepareMapper.class);
		job.setReducerClass(RealTimeFillerFreeBookPrepareReducer.class);
		job.setNumReduceTasks(1);	
		
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
		
		int ret = ToolRunner.run(new RealTimeFillerDataPrepareDriver(), args);	

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
