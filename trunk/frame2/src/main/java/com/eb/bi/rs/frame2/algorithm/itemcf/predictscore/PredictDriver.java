package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.algorithm.itemcf.similarity.TextPair;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class PredictDriver extends BaseDriver {

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
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = sdf.format(end);
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
		Configuration conf = null;
		Job job = null;
		FileSystem fs = null;
		FileStatus[] status = null;
		long start = 0;
		String inputPath = "";
		String outputPath = "";
		int reduceNum = Integer.parseInt(config.getParam("reduce_num", "20"));

		// 得到用户评分最高的N个物品
		if (config.getParam("if_select_useritemscore", true)) {
			log.info("STEP1: start to caculate top like item of user...");
			start = System.currentTimeMillis();
			conf = new Configuration(getConf());
			conf.set("mapreduce.map.cpu.vcores", "1");
			conf.set("mapreduce.output.textoutputformat.separator", "|");
			conf.set("conf.top.useritemscore.num",
					config.getParam("top_useritemscore_num", "20"));
			job = Job.getInstance(conf,
					"itemcf predict STEP1[caculate top like item of user]");
			job.setNumReduceTasks(reduceNum);
			job.setJarByClass(PredictDriver.class);
			job.setMapperClass(TopUserItemMapper.class);
			job.setReducerClass(TopUserItemReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			inputPath = config.getParam("user_item_score_output",
					"productization/itemcf/similarity/user_item_score_output");
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			outputPath = config.getParam("select_user_item_score_path",
					"productization/itemcf/predict/select_user_item_score");
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			check(outputPath);
			if (job.waitForCompletion(true)) {
				log.info("caculate top like item of user complete, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
			} else {
				log.error("caculate top like item of user failed, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
				return 1;
			}
		}

		// 取到出与源物品相似度大于min，且最大的N个相似物品
		log.info("STEP2: start to calculate top similar item...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vcores", "1");
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		conf.set("conf.min.similarity", config.getParam("min_similarity", "0"));
		conf.set("conf.top.similarity.num",
				config.getParam("top_similarity_num", "20"));
		job = Job.getInstance(conf,
				"itemcf predict STEP2[calculate top similary item]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopSimilarItemMapper.class);
		job.setReducerClass(TopSimilarItemReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		inputPath = config.getParam("item_similarity_path",
				"productization/itemcf/similarity/item_similariry_output");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("select_item_similarity_path",
				"productization/itemcf/predict/select_item_similarity");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("calculate top similar item complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("calculate top similar item failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 将用户物品打分与物品相似度进行内连接
		log.info("STEP3: start to join useritemscore with itemsimilarity matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job
				.getInstance(conf,
						"itemcf predict STEP3[join useritemscore with itemsimilarity matrix]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String selectUseritemscoreInputpath = config.getParam(
				"select_user_item_score_path", "productization/itemcf/predict/select_user_item_score");
		String itemSimilarityInputPath = config.getParam(
				"select_item_similarity_path", "productization/itemcf/predict/select_item_similarity");
		MultipleInputs.addInputPath(job,
				new Path(selectUseritemscoreInputpath), TextInputFormat.class,
				JoinScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(itemSimilarityInputPath),
				TextInputFormat.class, JoinSimilarityMapper.class);
		job.setReducerClass(JoinScoreAndSimilarityReducer.class);
		outputPath = config.getParam("score_similarity_join_path",
				"productization/itemcf/predict/score_similarity_join");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("join useritemscore with itemsimilarity matrix complete, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
		} else {
			log.error("join useritemscore with itemsimilarity matrix failed, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
			return 1;
		}

		// 预测用户物品得分
		log.info("STEP4: predict user item score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		conf.set("if_punish_hot_item",
				config.getParam("if_punish_hot_item", "false"));
		conf.set("predict_method",
				config.getParam("predict_method", "2"));
		job = Job.getInstance(conf,
				"itemcf predict STEP4[predict user item score]");
		job.setNumReduceTasks(reduceNum);
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(config.getParam("item_avg_output",
				"productization/itemcf/similarity/item_avg_output") + "/part-*"));
		for (FileStatus st : status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(PredictScoreMapper.class);
		job.setReducerClass(PredictScoreReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		inputPath = config.getParam("score_similarity_join_path", "productization/itemcf/predict/score_similarity_join");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_score_path", "productization/itemcf/predict/predict_score");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if (job.waitForCompletion(true)) {
			log.info("predict user item score complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("predict user item score failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 去除用户历史评分物品
		if (config.getParam("if_delete_history", true)) {
			log.info("STEP5: filter history of predict result...");
			start = System.currentTimeMillis();
			conf = new Configuration(getConf());
			conf.set("mapreduce.output.textoutputformat.separator", "|");
			job = Job.getInstance(conf,
					"itemcf predict STEP5[filter history of predict result]");
			job.setNumReduceTasks(reduceNum);
			job.setJarByClass(PredictDriver.class);
			job.setPartitionerClass(KeyPartition.class);
			job.setGroupingComparatorClass(TextPair.FirstComparator.class);
			job.setMapOutputKeyClass(TextPair.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(FilterPredictHistoryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			String predictScoreInputPath = config.getParam(
					"predict_score_path", "productization/itemcf/predict/predict_score");
			String ouserItemScoreInputpath = config.getParam(
					"user_item_score_output", "productization/itemcf/similarity/user_item_score_output");
			MultipleInputs.addInputPath(job, new Path(predictScoreInputPath),
					TextInputFormat.class, FilterPredictScoreMappper.class);
			MultipleInputs.addInputPath(job, new Path(ouserItemScoreInputpath),
					TextInputFormat.class, FilterHistoryMapper.class);

			outputPath = config.getParam("predict_score_filter_path", "productization/itemcf/predict/predict_score_filter");
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
		}

		// 取预测得分中最高的N个物品输出
		log.info("STEP6: start to caculate top predict item of user...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.map.cpu.vecores", "1");
		conf.set("mapreduce.input.fileinputformat.split.minsize",
				config.getParam("split_minsize", "268435456"));
		conf.set("conf.top.userpredictscore.num",
				config.getParam("top_userpredictscore_num", "50"));
		job = Job.getInstance(conf,
				"itemcf predict STEP6[caculate top predict item of user]");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(TopUserPredictItemMapper.class);
		job.setReducerClass(TopUserPredictItemReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 是否过滤用户历史评分数据
		if (config.getParam("if_delete_history", true)) {
			inputPath = config.getParam("predict_score_filter_path", "productization/itemcf/predict/predict_score_filter");
		} else {
			inputPath = config.getParam("predict_score_path", "productization/itemcf/predict/predict_score");
		}
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_result_path", "productization/itemcf/predict/predict_result");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		
		if (job.waitForCompletion(true)) {
			log.info("caculate top predict item of user complete, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
		} else {
			log.error("caculate top predict item of user failed, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
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
