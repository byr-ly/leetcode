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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

public class TestDriver extends Configured implements Tool {

	private static PluginUtil pluginUtil;
	private static Logger log;

	public TestDriver(String[] args) {
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

		int reduceNum = Integer.parseInt(config.getParam("num_reduce_tasks","20"));

		// 预测用户动漫得分计算
		log.info("STEP5: predict user opus score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf,
				"itemcf predict STEP5[predict user book score]");
		job.setNumReduceTasks(reduceNum);
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(config.getParam("opus_mean_score_path",
				"itemcf/predict/opus_mean_score") + "/part-*"));
		for (FileStatus st : status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}
		job.setNumReduceTasks(1);
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
				"itemcf/predict/score_similarity_join");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		outputPath = config.getParam("predict_score_path",
				"itemcf/predict/predict_score");
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
