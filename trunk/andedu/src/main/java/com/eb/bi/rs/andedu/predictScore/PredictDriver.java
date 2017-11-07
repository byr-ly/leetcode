package com.eb.bi.rs.andedu.predictScore;

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

import com.eb.bi.rs.andedu.combination.CombinationDriver;
import com.eb.bi.rs.andedu.predictScore.BrandPredictScoreMapper;
import com.eb.bi.rs.andedu.predictScore.JoinScoreAndSimilarityReducer;
import com.eb.bi.rs.andedu.predictScore.JoinScoreMapper;
import com.eb.bi.rs.andedu.predictScore.JoinSimilarityMapper;
import com.eb.bi.rs.andedu.predictScore.PredictScoreMapper;
import com.eb.bi.rs.andedu.predictScore.PredictScoreReducer;
import com.eb.bi.rs.andedu.predictScore.TopUserPredictBooksReducer;
import com.eb.bi.rs.andedu.predictScore.UserBrandMapper;
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
//		Date begin = new Date();
//
//		int ret = ToolRunner.run(new Configuration(), new PredictDriver(args), args);
//
//		Date end = new Date();
//		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//		String endTime = format.format(end);
//		long timeCost = end.getTime() - begin.getTime();
//
//		PluginResult result = pluginUtil.getResult();		
//		result.setParam("endTime", endTime);
//		result.setParam("timeCosts", timeCost);
//		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
//		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
//		result.save();
//
//		log.info("time cost in total(s): " + (timeCost / 1000.0));
//		System.exit(ret);
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(), new PredictDriver(args), args);

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
		System.exit(ret);
	}


	//@Override
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf = new Configuration(getConf());
		Job job;
		long start;
		String outputPath;
		
		
		
		
		//将相似度数据处理得到需要的数据格式如：品牌1|品牌2|相似分
		log.info("start to generate similarity score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		
		String ubs = config.getParam("sim_ret_output", "");///user/recsys/job0007/simi_ret_output
		String brandSimDir = config.getParam("brand_similarity_path", "");///user/recsys/job0008/pub_data/my_sim_ret_output
		//String weightDir = config.getParam("brand_score", "");
		System.out.println("输入路径:"+ubs);
		System.out.println("输出路径:"+brandSimDir);
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(PredictDriver.class);
		check(brandSimDir);
		
		FileInputFormat.setInputPaths(job, new Path(ubs));
		FileOutputFormat.setOutputPath(job, new Path(brandSimDir));
		
		job.setMapperClass(SimilarityMapper.class);
		//job.setReducerClass(BrandScoreReducer.class);
		
		//job.setNumReduceTasks(12);
		job.setNumReduceTasks(Integer.parseInt(config.getParam("sim_reduce_num", "5")));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
			log.info("generate user score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate user score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
		
		
		//根据仓库的yyyymmdd数据处理得到需要的：用户|资讯1|兴趣度1|资讯2|兴趣度2|…
		SimpleDateFormat date = new SimpleDateFormat("yyyyMMdd");
		String datenow=date.format(new Date());
		conf.set("datenow", datenow);
		log.info("start to generate interest score...");
		start = System.currentTimeMillis();
		
		String UserBrandScoreInputPath = config.getParam("user_brand_time_path", "");///user/recsys/job0008/user_action
		
		String interestScoreDir = config.getParam("user_brand_interest_score", "");///user/recsys/job0008/pub_data/user_brand_interest_score
		System.out.println("interestScoreDir是："+interestScoreDir);
		System.out.println("输入路径:"+UserBrandScoreInputPath);
		System.out.println("输出路径:"+interestScoreDir);
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(PredictDriver.class);
		check(interestScoreDir);
		
		FileInputFormat.setInputPaths(job, new Path(UserBrandScoreInputPath));
		FileOutputFormat.setOutputPath(job, new Path(interestScoreDir));
		
		job.setMapperClass(InterestMapper.class);
		job.setReducerClass(InterestReducer.class);
		
		job.setNumReduceTasks(config.getParam("generate_interest_score_reduce_num", 5));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
			log.info("generate interest score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate interest score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
		
		
		// 将用户品牌打分与品牌相似度进行内连接
		log.info("STEP1： start to join userbookscore with booksimilarity matrix...");
		start = System.currentTimeMillis();
		//conf = new Configuration(getConf());
		int join_task_reduce_num = Integer.parseInt(config.getParam("join_task_reduce_num", "5"));
		conf.set("similarity_limit", config.getParam("similarity_limit", "0.0"));
		
		//String UserBrandScoreInputPath = config.getParam("user_brand_time_path", "/user/recsys/job0008/user_action");
		String brandSimilarityInputPath = config.getParam("brand_similarity_path", "");///user/recsys/job0008/pub_data/my_sim_ret_output
		System.out.println("输入路径1:"+UserBrandScoreInputPath);
		System.out.println("输入路径2:"+brandSimilarityInputPath);
		
		outputPath = config.getParam("joined_user_brand_similarity", "");///user/recsys/job0008/pub_data/joined_user_brand_similarity
		System.out.println("输出路径:"+outputPath);
		
		job = Job.getInstance(conf, "itemcf predict STEP1|join userbookscore with booksimilarity matrix");
		job.setJarByClass(getClass());
		job.setNumReduceTasks(config.getParam("join_task_reduce_num", 5));
		
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
		
		String ubs2 = config.getParam("joined_user_brand_similarity", "");///user/recsys/job0008/pub_data/joined_user_brand_similarity
		String predictScoreDir = config.getParam("user_brand_predict_score", "");///user/recsys/job0008/pub_data/user_brand_predict_score
		System.out.println("输入路径:"+ubs2);
		System.out.println("输出路径:"+predictScoreDir);
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(PredictDriver.class);
		check(predictScoreDir);
		
		FileInputFormat.setInputPaths(job, new Path(ubs2));
		FileOutputFormat.setOutputPath(job, new Path(predictScoreDir));
		
		job.setMapperClass(PredictScoreMapper.class);
		job.setReducerClass(PredictScoreReducer.class);
		
		job.setNumReduceTasks(join_task_reduce_num);
		
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
		int filter_task_reduce_nums=Integer.parseInt(config.getParam("filter_task_reduce_nums", "5"));
		conf.set("conf.top.userpredictscore.num", config.getParam("top_n", "50"));
		
		//user_brand_score_path:格式：用户|品牌1|兴趣度1|品牌2|兴趣度2|... 计算过e^时间之后的
		//UserBrandScoreInputPath = config.getParam("user_brand_score_path", "");
		//String BrandPredictScoreInputPath = config.getParam("user_brand_predict_score", "/user/recsys/job0008/pub_data/user_brand_predict_score");
		
		System.out.println("输入路径1:"+interestScoreDir);
		System.out.println("输入路径2:"+predictScoreDir);
		
		outputPath = config.getParam("recommend_result_path", "");///user/recsys/job0008/recommend_result_guess_you_like
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
		
		MultipleInputs.addInputPath(job, new Path(interestScoreDir), TextInputFormat.class, UserBrandMapper.class);		
		MultipleInputs.addInputPath(job, new Path(predictScoreDir), TextInputFormat.class, BrandPredictScoreMapper.class);
		
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

