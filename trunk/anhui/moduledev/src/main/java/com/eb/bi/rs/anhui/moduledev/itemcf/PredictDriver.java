package com.eb.bi.rs.anhui.moduledev.itemcf;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
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
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.BrandPredictScoreMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.JoinScoreAndSimilarityReducer;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.JoinScoreMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.JoinSimilarityMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.PredictScoreMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.PredictScoreReducer;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.TopUserPredictBooksReducer;
import com.eb.bi.rs.anhui.moduledev.itemcf.predictscore.UserBrandMapper;

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
		long start;
		String outputPath;

		// 将用户品牌打分与品牌相似度进行内连接
		log.info("STEP1： start to join userbookscore with booksimilarity matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		int join_task_reduce_num = Integer.parseInt(config.getParam("join_task_reduce_num", "20"));
		conf.set("similarity_limit", config.getParam("similarity_limit", "0.05"));
		
		String UserBrandScoreInputPath = config.getParam("user_brand_score_path", "");
		String brandSimilarityInputPath = config.getParam("brand_similarity_path", "");
		System.out.println("输入路径1:"+UserBrandScoreInputPath);
		System.out.println("输入路径2:"+brandSimilarityInputPath);
		
		outputPath = config.getParam("joined_user_brand_similarity", "");
		System.out.println("输出路径:"+outputPath);
		
		job = Job.getInstance(conf, "itemcf predict STEP1|join userbookscore with booksimilarity matrix");
		job.setJarByClass(getClass());
		job.setNumReduceTasks(join_task_reduce_num);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(JoinScoreAndSimilarityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(job, new Path(UserBrandScoreInputPath), TextInputFormat.class, JoinScoreMapper.class);		
		MultipleInputs.addInputPath(job, new Path(brandSimilarityInputPath), TextInputFormat.class, JoinSimilarityMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);
		if( job.waitForCompletion(true)){
			log.info("join userbookscore with booksimilarity matrix complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("join userbookscore with booksimilarity matrix failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}

		//求目的品牌预测评分
		log.info("start to generate predict score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		
		String ubs = config.getParam("joined_user_brand_similarity", "");
		String predictScoreDir = config.getParam("user_brand_predict_score", "");
		System.out.println("输入路径:"+ubs);
		System.out.println("输出路径:"+predictScoreDir);
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(SimilarityDriver.class);
		check(predictScoreDir);
		
		FileInputFormat.setInputPaths(job, new Path(ubs));
		FileOutputFormat.setOutputPath(job, new Path(predictScoreDir));
		
		job.setMapperClass(PredictScoreMapper.class);
		job.setReducerClass(PredictScoreReducer.class);
		
		job.setNumReduceTasks(config.getParam("generate_predict_score_reduce_num", 12));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
			log.info("generate predict score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate predict score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}

		//过滤用户评过分的品牌，并取预测得分中最高的N个品牌输出
		log.info("STEP2: start to caculate top predict books of user...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		int filter_task_reduce_nums=Integer.parseInt(config.getParam("filter_task_reduce_nums", "20"));
		conf.set("conf.top.userpredictscore.num", config.getParam("top_n", "50"));
		
		UserBrandScoreInputPath = config.getParam("user_brand_score_path", "");
		String BrandPredictScoreInputPath = config.getParam("user_brand_predict_score", "");
		
		System.out.println("输入路径1:"+UserBrandScoreInputPath);
		System.out.println("输入路径2:"+BrandPredictScoreInputPath);
		
		outputPath = config.getParam("recommend_result_path", "");
		System.out.println("输出路径:"+outputPath);
		
		job = Job.getInstance(conf, "itemcf predict STEP2[caculate top predict books of user]");
		job.setJarByClass(PredictDriver.class);
		job.setNumReduceTasks(filter_task_reduce_nums);
		job.setReducerClass(TopUserPredictBooksReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(job, new Path(UserBrandScoreInputPath), TextInputFormat.class, UserBrandMapper.class);		
		MultipleInputs.addInputPath(job, new Path(BrandPredictScoreInputPath), TextInputFormat.class, BrandPredictScoreMapper.class);
		
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
	
}

