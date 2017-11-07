package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.FloatWritable;
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

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;

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

		int ret = ToolRunner.run(new Configuration(), new PredictDriver(args),
				args);

		Date end = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(end);
		long timeCost = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC
				: PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully"
				: "run failed.");
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
		FileStatus[] status = null;
		long start = 0;
		String inputPath = "";
		String outputPath = "";

		// 计算各个动漫的平均打分
		log.info("STEP1: start to caculate mean scores of opus...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1"); 
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf,
				"itemcf predict STEP1[caculate mean score of opus]");
		int reduceNum = Integer.parseInt(config.getParam("num_reduce_tasks",
				"20"));
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(OpusMeanScoreMapper.class);
		job.setReducerClass(OpusMeanScoreReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("user_opus_score_output",
				"recsys/itemcf/similarity/user_opus_score_output");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("opus_mean_score_path",
				"recsys/itemcf/predict/opus_mean_score");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("caculate mean scores of opus complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("caculate mean scores of opus failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 得到用户评分最高的N个动漫
		if (config.getParam("if_select_useropuscore", true)) {
			log.info("STEP2: start to caculate top like opus of user...");
			start = System.currentTimeMillis();
			conf = new Configuration(getConf());
			conf.set("mapreduce.map.cpu.vcores", "1"); 
			conf.set("mapreduce.output.textoutputformat.separator", "|");
			conf.set("conf.top.useropuscore.num",
					config.getParam("top_useropuscore_num", "20"));
			job = Job.getInstance(conf,
					"itemcf predict STEP2[caculate top like opus of user]");
			job.setNumReduceTasks(reduceNum);
			job.setJarByClass(PredictDriver.class);
			job.setMapperClass(TopUserOpusMapper.class);
			job.setReducerClass(TopUserOpusReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			inputPath = config.getParam("user_opus_score_output",
					"recsys/itemcf/similarity/user_opus_score_output");
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			outputPath = config.getParam("select_user_opus_score_path",
					"recsys/itemcf/predict/select_user_opus_score");
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			check(outputPath);
			
			if (job.waitForCompletion(true)) {
				log.info("caculate top like opus of user complete, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
			} else {
				log.error("caculate top like opus of user failed, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
				return 1;
			}
		}

		// 取到出与源动漫相似度大于min，且最大的N个相似动漫
		log.info("STEP3: start to calculate top similar opus...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1"); 
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		conf.set("conf.min.similarity", config.getParam("min_similarity", "0"));
		conf.set("conf.top.similarity.num",
				config.getParam("top_similarity_num", "20"));
		job = Job.getInstance(conf,
				"itemcf predict STEP3[calculate top similary opus]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopSimilarOpusMapper.class);
		job.setReducerClass(TopSimilarOpusReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		inputPath = config.getParam("opus_similarity_path",
				"recsys/itemcf/similarity/opus_similariry_output");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("select_opus_similarity_path",
				"recsys/itemcf/predict/select_opus_similarity");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("calculate top similar opus complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("calculate top similar opus failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 将用户动漫打分与动漫相似度进行内连接
		log.info("STEP4： start to join useropuscore with opusimilarity matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1"); 
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job
				.getInstance(conf,
						"itemcf predict STEP4[join useropuscore with opusimilarity matrix]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(getClass());
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(JoinScoreAndSimilarityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String selectUseropuscoreInputPath = config.getParam(
				"select_user_opus_score_path",
				"recsys/itemcf/predict/select_user_opus_score");
		String opusimilarityInputPath = config.getParam(
				"select_opus_similarity_path",
				"recsys/itemcf/predict/select_opus_similarity");
		outputPath = config.getParam("score_similarity_join_path",
				"recsys/itemcf/predict/score_similarity_join");
		MultipleInputs.addInputPath(job, new Path(selectUseropuscoreInputPath),
				TextInputFormat.class, JoinScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(opusimilarityInputPath),
				TextInputFormat.class, JoinSimilarityMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("join useropuscore with opusimilarity matrix complete, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
		} else {
			log.error("join useropuscore with opusimilarity matrix failed, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
			return 1;
		}

		// 预测用户动漫得分计算
		log.info("STEP5: predict user opus score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1");  
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		conf.set("mapreduce.input.fileinputformat.split.minsize", config.getParam("split_minsize", "268435456"));
		job = Job.getInstance(conf,
				"itemcf predict STEP5[predict user opus score]");
		job.setNumReduceTasks(reduceNum);
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(config.getParam("opus_mean_score_path",
				"recsys/itemcf/predict/opus_mean_score") + "/part-*"));
		for (FileStatus st : status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(PredictScoreMapper.class);
		job.setReducerClass(PredictScoreReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("score_similarity_join_path",
				"recsys/itemcf/predict/score_similarity_join");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_score_path",
				"recsys/itemcf/predict/predict_score");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("calculate top similar opus complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("calculate top similar opus failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 将预测用户动漫得分中的用户评过分的动漫删除
		log.info("STEP6： filter history of predict result...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1"); 
		conf.set("mapreduce.input.fileinputformat.split.minsize", config.getParam("split_minsize", "268435456"));
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf,
				"itemcf predict STEP6[filter history of predict result]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(getClass());
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FilterPredictHistoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String predictScoreInputPath = config.getParam("predict_score_path",
				"recsys/itemcf/predict/predict_score");
		String useropuscoreInputPath = config.getParam(
				"user_opus_score_output",
				"recsys/itemcf/similarity/user_opus_score_output");
		outputPath = config.getParam("predict_score_filter_path",
				"recsys/itemcf/predict/predict_score_filter");
		MultipleInputs.addInputPath(job, new Path(predictScoreInputPath),
				TextInputFormat.class, FilterPredictScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(useropuscoreInputPath),
				TextInputFormat.class, FilterHistoryMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("filter history of predict result complete, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
		} else {
			log.error("filter history of predict result failed, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
			return 1;
		}

		// 取预测得分中最高的N个动漫输出
		log.info("STEP7: start to caculate top predict opus of user...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1"); 
		conf.set("mapreduce.input.fileinputformat.split.minsize", config.getParam("split_minsize", "268435456"));
		conf.set("conf.top.userpredictscore.num",
				config.getParam("top_userpredictscore_num", "50"));
		conf.set("ranks", config.getParam("ranks", "S,A,s,a"));
		job = Job.getInstance(conf,
				"itemcf predict STEP7[caculate top predict opus of user]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopUserPredictOpusMapper.class);
		job.setReducerClass(TopUserPredictOpusReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		inputPath = config.getParam("predict_score_filter_path",
				"recsys/itemcf/predict/predict_score_filter");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_result_path",
				"recsys/itemcf/predict/predict_result");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		
		//加入动漫信息到cache中
		String cachePath = config.getParam("opus_info_path",
				"/user/eb/dump_data/recsys/dim_opus/*");
		if (cachePath != null) {
			conf.set("opusinfo.cache.path", cachePath);
		}
		fs = FileSystem.get(URI.create(cachePath), conf);
		status = fs.listStatus(new Path(cachePath));
		for (FileStatus fss : status) {
			job.addCacheFile(URI.create(fss.getPath().toString()));
			log.info(fss.getPath().toString() + " has been add into cache.");
		}
		
		if (job.waitForCompletion(true)) {
			log.info("caculate top predict opus of user complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("caculate top predict opus of user failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		return 0;
	}

	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName),
					new Configuration());
			Path f = new Path(fileName);
			boolean isExists = fs.exists(f);
			if (isExists) { // if exists, delete
				boolean isDel = fs.delete(f, true);
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
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}
}
