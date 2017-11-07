package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class CaculateSimilarityDriver extends BaseDriver {

	private static PluginUtil pluginUtil;
	private static Logger log;

	public CaculateSimilarityDriver(String[] args) {
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}

	public static void main(String[] args) throws Exception {
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(),
				new CaculateSimilarityDriver(args), args);

		Date end = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = sdf.format(end);
		long costTime = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", costTime);
		result.setParam("exitCode", ret == 0 ? "run successfully"
				: "run failed");
		result.save();

		log.info("time cost in total(s): " + (costTime / 1000.0));
		System.exit(0);

	}

	@Override
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf = null;
		Job job = null;
		long start = 0;

		int reduceNum = Integer.parseInt(config.getParam("reduce_num", "5"));
		String userItemScoreOutput = config.getParam("user_item_score_output",
				"productization/itemcf/similarity/user_item_score_output");
		boolean isChange = config.getParam("is_change", false);

		// 是否考虑用户评分平均值的影响
		if (isChange) {
			start = System.currentTimeMillis();
			log.info("start to caculate the average user to score...");
			conf = new Configuration(getConf());
			conf.set("mapreduce.output.textoutputformat.separator", "|");
			job = Job.getInstance(conf, "caculate average");
			job.setJarByClass(CaculateSimilarityDriver.class);
			job.setMapperClass(UserItemMeanScoreMapper.class);
			job.setReducerClass(UserItemMeanScoreReducer.class);
			job.setNumReduceTasks(reduceNum);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(userItemScoreOutput));
			String userItemMeanScorePath = config.getParam(
					"user_item_avg_output",
					"productization/itemcf/similarity/user_item_avg_output");
			FileOutputFormat
					.setOutputPath(job, new Path(userItemMeanScorePath));
			check(userItemMeanScorePath);

			if (job.waitForCompletion(true)) {
				log.info("caculate the average user to score complete, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
			} else {
				log.error("caculate the average user to score failed, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
				return 1;
			}

			start = System.currentTimeMillis();
			log.info("start to generate new user item score matrix...");
			conf = new Configuration(getConf());
			conf.set("mapreduce.output.textoutputformat.separator", "|");
			job = Job.getInstance(conf, "generate new user item score");
			job.setPartitionerClass(KeyPartition.class);
			job.setGroupingComparatorClass(TextPair.FirstComparator.class);
			job.setJarByClass(CaculateSimilarityDriver.class);
			job.setReducerClass(GenerateNewUserItemScoreReducer.class);
			job.setNumReduceTasks(reduceNum);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(TextPair.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			MultipleInputs.addInputPath(job, new Path(userItemMeanScorePath),
					TextInputFormat.class, UserMeanScoreMapper.class);
			MultipleInputs.addInputPath(job, new Path(userItemScoreOutput),
					TextInputFormat.class, UserItemScoreMapper.class);
			String changeUserItemScoreOutput = config
					.getParam("change_user_item_score_output",
							"productization/itemcf/similarity/change_user_item_score_output");
			FileOutputFormat.setOutputPath(job, new Path(
					changeUserItemScoreOutput));
			check(changeUserItemScoreOutput);

			if (job.waitForCompletion(true)) {
				log.info("generate new user item score matrix complete, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
			} else {
				log.error("generate new user item score matrix failed, time cost: "
						+ (System.currentTimeMillis() - start) / 1000 + "s");
				return 1;
			}
			userItemScoreOutput = changeUserItemScoreOutput;
		}

		// 计算物品的模
		start = System.currentTimeMillis();
		log.info("start to caculate item's module...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "caculate module");
		job.setJarByClass(CaculateSimilarityDriver.class);
		job.setMapperClass(ItemModuleMapper.class);
		job.setReducerClass(ItemModuleReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(userItemScoreOutput));
		String itemModuleDir = config.getParam("item_module_output",
				"productization/itemcf/similarity/item_module_output");
		FileOutputFormat.setOutputPath(job, new Path(itemModuleDir));
		check(itemModuleDir);

		if (job.waitForCompletion(true)) {
			log.info("caculate item's module complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("caculate item's module failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 计算物品的平均分
		start = System.currentTimeMillis();
		log.info("start to caculate item's average score...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "caculate module");
		job.setJarByClass(CaculateSimilarityDriver.class);
		job.setMapperClass(ItemAvgMapper.class);
		job.setReducerClass(ItemAvgReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(userItemScoreOutput));
		String itemAvgDir = config.getParam("item_avg_output",
				"productization/itemcf/similarity/item_avg_output");
		FileOutputFormat.setOutputPath(job, new Path(itemAvgDir));
		check(itemAvgDir);

		if (job.waitForCompletion(true)) {
			log.info("caculate item's average score complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("caculate item's average score failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 两两物品组合
		log.info("start to combinate user score...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "combination");
		job.setJarByClass(CaculateSimilarityDriver.class);
		job.setMapperClass(UserScoreMapper.class);
		job.setReducerClass(UserScoreReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(userItemScoreOutput));
		String scoreTmpDir = "productization/itemcf/user_score_"
				+ System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(scoreTmpDir));
		check(scoreTmpDir);

		if (job.waitForCompletion(true)) {
			log.info("combinate user score complete, time cost : "
					+ +(System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("combinate user score failed, time cost : "
					+ +(System.currentTimeMillis() - start) / 1000 + "s");
			return -1;
		}

		// 相似度计算方法三种：cosine,欧式距离,Pearson相关系数
		String simi_method = config.getParam("simi_method", "cos");
		log.info("start to generate similarity matrix...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		conf.set("cos_method", config.getParam("cos_method", "1"));
		conf.set("if_punish_active_users",
				config.getParam("if_punish_active_users", "false"));
		job = Job.getInstance(conf, "similarity");
		job.setJarByClass(CaculateSimilarityDriver.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = null;
		
		if ("cos".equals(simi_method)) {
			job.setMapOutputValueClass(CosineSimilarityWritable.class);
			job.setMapperClass(CosineSimilarityMapper.class);
			job.setReducerClass(CosineSimilarityReducer.class);
			status = fs.globStatus(new Path(itemModuleDir + "/part-*"));
			for (FileStatus st : status) {
				job.addCacheFile(new Path(st.getPath().toString()).toUri());
			}
		} else if ("pearson".equals(simi_method)) {
			job.setMapOutputValueClass(PearsonSimilarityWritable.class);
			job.setMapperClass(PearsonSimilarityMapper.class);
			job.setReducerClass(PearsonSimilarityReducer.class);
			status = fs.globStatus(new Path(itemAvgDir + "/part-*"));
			for (FileStatus st : status) {
				job.addCacheFile(new Path(st.getPath().toString()).toUri());
			}
		} else if ("euclidean".equals(simi_method)) {
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setMapperClass(EuclideanSimilarityMapper.class);
			job.setReducerClass(EuclideanSimilarityReducer.class);
		}

		FileInputFormat.setInputPaths(job, new Path(scoreTmpDir + "/part-*"));
		String itemSimDir = config.getParam("item_similariry_output",
				"productization/itemcf/similarity/item_similariry_output");
		FileOutputFormat.setOutputPath(job, new Path(itemSimDir));
		check(itemSimDir);

		if (job.waitForCompletion(true)) {
			log.info("generate similarity matrix complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("generate similarity matrix failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}
		// check(scoreTmpDir);
		return 0;
	}

	/**
	 * 检查输出目录是否存在，若存在则删除
	 */
	public void check(String filePath) {
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath),
					new Configuration());
			Path path = new Path(filePath);
			boolean isExists = fs.exists(path);
			if (isExists) {
				boolean isDel = fs.delete(path, true);
				log.info(filePath + " delete? \t" + isDel);
			} else {
				log.info(filePath + " exist? \t" + isExists);
			}
		} catch (Exception e) {
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
