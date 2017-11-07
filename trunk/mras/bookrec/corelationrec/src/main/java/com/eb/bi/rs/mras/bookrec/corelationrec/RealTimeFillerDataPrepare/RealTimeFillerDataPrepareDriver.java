package com.eb.bi.rs.mras.bookrec.corelationrec.RealTimeFillerDataPrepare;

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

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

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
		 **		作者热书、分类热书、大类热书
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		String bookAuthorClassFrequencyPath = config.getParam("read_class_frequency_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("hot.book.count", config.getParam("read_hot_book_count", 200));		
		conf.set("author.book.output.path", config.getParam("read_author_book_path", ""));
		conf.set("class.book.output.path", config.getParam("read_class_book_path", ""));
		conf.set("bigclass.book.output.path", config.getParam("read_bigclass_book_path", ""));
				

	
		job = new Job(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		String outputPath = config.getParam("read_output_path", "");;
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
		 **		1.输入路径：浏览图书信息频次表
		 ** 输出：
		 **		作者热书、分类热书、大类热书
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		bookAuthorClassFrequencyPath = config.getParam("browse_class_frequency_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("hot.book.count", config.getParam("hot_book_count", 200));		
		conf.set("author.book.output.path", config.getParam("browse_author_book_path", ""));
		conf.set("class.book.output.path", config.getParam("browse_class_book_path", ""));
		conf.set("bigclass.book.output.path", config.getParam("browse_bigclass_book_path", ""));
				

	
		job = new Job(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		outputPath = config.getParam("browse_output_path", "");;
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
		 **		作者热书、分类热书、大类热书
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		bookAuthorClassFrequencyPath = config.getParam("order_class_frequency_path", "");
		
		conf = new Configuration(getConf());
		conf.setInt("hot.book.count", config.getParam("hot_book_count", 200));		
		conf.set("author.book.output.path", config.getParam("order_author_book_path", ""));
		conf.set("class.book.output.path", config.getParam("order_class_book_path", ""));
		conf.set("bigclass.book.output.path", config.getParam("order_bigclass_book_path", ""));
				

	
		job = new Job(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		outputPath = config.getParam("order_output_path", "");;
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
		 **		1.输入路径：免费图书表
		 ** 输出：
		 **		免费图书表
		 ** 功能描述：
		 ** 	为实时补白部分作数据准备

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		

		String freebookPath = config.getParam("free_book_path", "");	
		conf = new Configuration(getConf());
		job = new Job(conf,"CorelationRecommend-realtime_filler_data_prepare");		
		job.setJarByClass(getClass());		
	
		outputPath = config.getParam("free_book_output_path", "");;
		check(outputPath);	
		
		FileInputFormat.setInputPaths(job, new Path(freebookPath ));	
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		log.info("job input path: " + freebookPath);
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
