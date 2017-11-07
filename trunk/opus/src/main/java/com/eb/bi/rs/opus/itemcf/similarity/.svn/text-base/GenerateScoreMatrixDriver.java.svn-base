package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
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

public class GenerateScoreMatrixDriver extends Configured implements Tool {

	private static PluginUtil pluginUtil;
	private static Logger log;

	public GenerateScoreMatrixDriver(String[] args) {
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}

	public static void main(String[] args) throws Exception {
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(),
				new GenerateScoreMatrixDriver(args), args);

		Date end = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = sdf.format(end);
		long costTime = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", costTime);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC
				: PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully"
				: "run failed");
		result.save();

		log.info("time cost in total(s): " + (costTime / 1000.0));
		System.exit(ret);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf = null;
		Job job = null;
		long start = 0;

		// 生成用户动漫评分矩阵
		log.info("start generate score matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		conf.set("downloadWeight", config.getParam("downloadWeight", "0.45"));
		conf.set("collectWeight", config.getParam("collectWeight", "0.35"));
		conf.set("onlineplayWeight",
				config.getParam("onlineplayWeight", "0.15"));
		conf.set("clickWeight", config.getParam("clickWeight", "0.05"));
		int reduceNum = Integer.parseInt(config.getParam("reduce_num", "5"));
		job = Job.getInstance(conf, "generate_score_matrix");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(GenerateScoreMatrixDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		String downloadInput = config.getParam("downloadInputPath", "");
		String collectInput = config.getParam("collectInputPath", "");
		String onlineplayInput = config.getParam("onlineplayInputPath", "");
		String clickInput = config.getParam("clickInputPath", "");
		MultipleInputs.addInputPath(job, new Path(downloadInput),
				TextInputFormat.class, DownLoadMatrixMappper.class);
		MultipleInputs.addInputPath(job, new Path(collectInput),
				TextInputFormat.class, CollectMatrixMappper.class);
		MultipleInputs.addInputPath(job, new Path(onlineplayInput),
				TextInputFormat.class, OnlineplayMatrixMappper.class);
		MultipleInputs.addInputPath(job, new Path(clickInput),
				TextInputFormat.class, ClickMatrixMappper.class);

		job.setReducerClass(GenerateScoreMatrixReducer.class);

		String userOpusScoreOutput = config.getParam("user_opus_score_output",
				"recsys/itemcf/similarity/user_opus_score_output");
		FileOutputFormat.setOutputPath(job, new Path(userOpusScoreOutput));
		check(userOpusScoreOutput);

		if (job.waitForCompletion(true)) {
			log.info("generate score matrix complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.info("generate score matrix failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 计算动漫的模
		log.info("start to caculate opus's module...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "caculate module");
		job.setJarByClass(GenerateScoreMatrixDriver.class);
		job.setMapperClass(OpusModuleMapper.class);
		job.setReducerClass(OpusModuleReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(userOpusScoreOutput));
		String opusModuleDir = config.getParam("opus_module_output",
				"recsys/itemcf/similarity/opus_module_output");
		FileOutputFormat.setOutputPath(job, new Path(opusModuleDir));
		check(opusModuleDir);
		if (job.waitForCompletion(true)) {
			log.info("caculate opus's module complete, time cost : "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.info("caculate opus's module failed, time cost : "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 两两动漫数据组合
		log.info("start to combinate user score...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "combination");
		job.setJarByClass(GenerateScoreMatrixDriver.class);
		job.setMapperClass(UserScoreMapper.class);
		job.setReducerClass(UserScoreReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(userOpusScoreOutput));
		String scoreDir = "recsys/user_score_" + System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(scoreDir));
		check(scoreDir);

		if (job.waitForCompletion(true)) {
			log.info("combinate user score complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.info("combinate user score failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}

		// 计算动漫相似度
		log.info("start to generate similarity matrix...");
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "similarity");
		job.setJarByClass(GenerateScoreMatrixDriver.class);
		job.setMapperClass(SimilariryMapper.class);
		job.setReducerClass(SimilarityReducer.class);
		job.setNumReduceTasks(reduceNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SimilarityWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(opusModuleDir + "/part-*"));
		for (FileStatus st : status) {
			job.addCacheFile(new Path(st.getPath().toString()).toUri());
		}
		
		FileInputFormat.setInputPaths(job, new Path(scoreDir + "/part-*"));
		String opusSimDir = config.getParam("opus_similariry_output",
				"recsys/itemcf/similarity/opus_similariry_output");
		FileOutputFormat.setOutputPath(job, new Path(opusSimDir));
		check(opusSimDir);

		if (job.waitForCompletion(true)) {
			log.info("generate similarity matrix complete, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
		} else {
			log.error("generate similarity matrix failed, time cost: "
					+ (System.currentTimeMillis() - start) / 1000 + "s");
			return 1;
		}
		// check(scoreDir);
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
}
