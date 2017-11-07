package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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

public class PredictDriver extends Configured implements Tool {

	private static PluginUtil pluginUtil;
	private static Logger log;

	public PredictDriver(String[] args) {
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}

	public static void main(String[] args) throws Exception {
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(), new PredictDriver(args), args);

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


	//@Override
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		Job job;
		FileSystem fs;
		FileStatus[] status;
		long start;
		String inputPath, outputPath;

		// 计算各个图书的平均打分
		log.info("STEP1: start to caculate mean scores of books...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf, "itemcf predict STEP1[caculate mean score of books]");
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(BookMeanScoreMapper.class);
		job.setReducerClass(BookMeanScoreReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("user_book_score_path", "similarity/input/user_book_score");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("book_mean_score_path", "predict/book_mean_score");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("caculate mean scores of books complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("caculate mean scores of books failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}

		// 得到用户评分最高的N本书
		if (config.getParam("if_select_userbookscore", true)) {
			log.info("STEP2: start to caculate top like books of user...");
			start = System.currentTimeMillis();
			conf = new Configuration(getConf());
			conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
			conf.set("mapred.textoutputformat.separator", "|");
			conf.set("conf.top.userbookscore.num", config.getParam("top_userbookscore_num", "20"));
			job = new Job(conf, "itemcf predict STEP2[caculate top like books of user]");
			job.setJarByClass(PredictDriver.class);
			job.setMapperClass(TopUserBooksMapper.class);
			job.setReducerClass(TopUserBooksReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			inputPath = config.getParam("user_book_score_path", "similarity/input/user_book_score");
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			outputPath = config.getParam("select_user_book_score_path", "predict/select_user_book_score");
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			check(outputPath);
			if (job.waitForCompletion(true)) {
				log.info("caculate top like books of user complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			} else {
				log.error("caculate top like books of user failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
				return 1;
			}
		}
		
		// 取到出与源图书相似度大于min，且最大的N本相似图书
		log.info("STEP3: start to calculate top similar books...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.min.similarity", config.getParam("min_similarity", "0"));
		conf.set("conf.top.similarity.num", config.getParam("top_similarity_num", "20"));
		job = new Job(conf, "itemcf predict STEP3[calculate top similary books]");
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopSimilarBooksMapper.class);
		job.setReducerClass(TopSimilarBooksReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		inputPath = config.getParam("book_similarity_path", "similarity/book_similarity");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("select_book_similarity_path", "predict/select_book_similarity");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("calculate top similar books complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("calculate top similar books failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}

		
		// 将用户图书打分与图书相似度进行内连接
		log.info("STEP4： start to join userbookscore with booksimilarity matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf,"itemcf predict STEP4|join userbookscore with booksimilarity matrix");
		job.setJarByClass(getClass());
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(JoinScoreAndSimilarityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String selectUserBookScoreInputPath = config.getParam("select_user_book_score_path", "predict/select_user_book_score");
		String bookSimilarityInputPath = config.getParam("select_book_similarity_path", "predict/select_book_similarity");
		outputPath = config.getParam("score_similarity_join_path", "predict/score_similarity_join");
		MultipleInputs.addInputPath(job, new Path(selectUserBookScoreInputPath), TextInputFormat.class, JoinScoreMapper.class);		
		MultipleInputs.addInputPath(job, new Path(bookSimilarityInputPath), TextInputFormat.class, JoinSimilarityMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if( job.waitForCompletion(true)){
			log.info("join userbookscore with booksimilarity matrix complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("join userbookscore with booksimilarity matrix failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}

		// 预测用户图书得分计算 
		log.info("STEP5: predict user book score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(config.getParam("book_mean_score_path", "predict/book_mean_score") + "/part-*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}
		job = new Job(conf, "itemcf predict STEP5[predict user book score]");
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(PredictScoreMapper.class);
		job.setReducerClass(PredictScoreReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("score_similarity_join_path", "predict/score_similarity_join");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_score_path", "predict/predict_score");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("calculate top similar books complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("calculate top similar books failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
		
		// 将预测用户图书得分中的用户评过分的图书删除
		log.info("STEP6： filter history of predict result...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		job = new Job(conf,"itemcf predict STEP6|filter history of predict result");
		job.setJarByClass(getClass());
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FilterPredictHistoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String predictScoreInputPath = config.getParam("predict_score_path", "predict/predict_score");
		String userBookScoreInputPath = config.getParam("user_book_score_path", "similarity/input/user_book_score");
		outputPath = config.getParam("predict_score_filter_path", "predict/predict_score_filter");
		MultipleInputs.addInputPath(job, new Path(predictScoreInputPath), TextInputFormat.class, FilterPredictScoreMapper.class);		
		MultipleInputs.addInputPath(job, new Path(userBookScoreInputPath), TextInputFormat.class, FilterHistoryMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if( job.waitForCompletion(true)){
			log.info("filter history of predict result complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("filter history of predict result failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}

		// 取预测得分中最高的N本图书输出
		log.info("STEP7: start to caculate top predict books of user...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapred.reduce.tasks", config.getParam("num_reduce_tasks", "20"));
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.top.userpredictscore.num", config.getParam("top_userpredictscore_num", "50"));
		job = new Job(conf, "itemcf predict STEP7[caculate top predict books of user]");
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopUserPredictBooksMapper.class);
		job.setReducerClass(TopUserPredictBooksReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("predict_score_filter_path", "predict/predict_score_filter");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_result_path", "predict/predict_result");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("caculate top predict books of user complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("caculate top predict books of user failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}

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
	
	public static class KeyPartition extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)% numPartitions;
		}
	}
}

