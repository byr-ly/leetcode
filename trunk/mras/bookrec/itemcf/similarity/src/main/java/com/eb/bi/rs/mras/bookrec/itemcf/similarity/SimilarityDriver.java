package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

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

public class SimilarityDriver extends Configured implements Tool {

	private static PluginUtil pluginUtil;
	private static Logger log;

	public SimilarityDriver(String[] args) {
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}

	public static void main(String[] args) throws Exception {
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(), new SimilarityDriver(args), args);

		Date end = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(end);
		long timeCost = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();

		log.info("time cost in total(s): " + (timeCost / 1000.0));
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		Job job;
		FileSystem fs;
		FileStatus[] status;
		long start;

		// 过滤阅读用户数、订购用户数、深度阅读用户数任意一项指标为0的数据
		log.info("start to filter zero record...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(FilterMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String inputPath = config.getParam("book_statistics_info", "similarity/input/book_statistics_info");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		String filterDir = config.getParam("filter_book_dir", "similarity/filter_book_statistics_info");
		FileOutputFormat.setOutputPath(job, new Path(filterDir));
		check(filterDir);
		if (job.waitForCompletion(true)) {
			log.info("filter zero record complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("filter zero record failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}


		// 计算指标的十分位数
		log.info("start to calculate tenth number...");
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(TenthNumMapper.class);
		job.setReducerClass(TenthNumReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(filterDir + "/part-*"));
		String tenthDir = config.getParam("tenth_num_dir", "similarity/statstics_tenth_num");
		FileOutputFormat.setOutputPath(job, new Path(tenthDir));
		check(tenthDir);
		if (job.waitForCompletion(true)) {
			log.info("calculate tenth number complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("calculate tenth number failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}


		// 过滤掉非流行图书
		log.info("start to generate populate book...");
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(tenthDir + "/part-*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(PopBookMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(filterDir + "/part-*"));
		String popBookDir = config.getParam("pop_book_dir", "similarity/pop_book");
		FileOutputFormat.setOutputPath(job, new Path(popBookDir));
		check(popBookDir);
		if (job.waitForCompletion(true)) {
			log.info("generate populate book complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate populate book failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}


		// 计算流行图书的权重
		log.info("start to calculate populate book's weight...");
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(PopWeightMapper.class);
		job.setReducerClass(PopWeightReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(popBookDir));
		String popWeightDir = config.getParam("pop_weight_dir", "similarity/book_pop_weight");
		FileOutputFormat.setOutputPath(job, new Path(popWeightDir));
		check(popWeightDir);
		if (job.waitForCompletion(true)) {
			log.info("calculate populate book's weight complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("calculate populate book's weight failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}


		// 处理用户评分数据
		log.info("start to generate user score...");
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		status = fs.globStatus(new Path(popWeightDir + "/part-*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(UserScoreMapper.class);
		job.setReducerClass(UserScoreReducer.class);
		int reduceNum = Integer.parseInt(config.getParam("num_reduce_tasks", "12"));
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		String ubs = config.getParam("user_book_score", "similarity/input/user_book_score");
		FileInputFormat.setInputPaths(job, new Path(ubs));
		String scoreDir = "user_score_" + System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(scoreDir));
		check(scoreDir);
		if (job.waitForCompletion(true)) {
			log.info("generate user score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate user score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}


		// 计算图书的相似度
		log.info("start to generate similarity matrix...");
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		status = fs.globStatus(new Path(popWeightDir + "/part-*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}
		job = new Job(conf, "similarity");
		job.setJarByClass(SimilarityDriver.class);
		job.setMapperClass(SimilarityMapper.class);
		job.setReducerClass(SimilarityReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SimilarityWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(scoreDir + "/part-*"));
		String bookSimDir = config.getParam("book_similarity_dir", "similarity/book_similarity");
		FileOutputFormat.setOutputPath(job, new Path(bookSimDir));
		check(bookSimDir);
		if (job.waitForCompletion(true)) {
			log.info("generate similarity matrix complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate similarity matrix failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
		check(scoreDir);

		return 0;
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
