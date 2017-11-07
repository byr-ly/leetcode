package com.eb.bi.rs.anhui.moduledev.itemcf;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.anhui.moduledev.itemcf.similarity.BrandScoreMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.similarity.BrandScoreReducer;
import com.eb.bi.rs.anhui.moduledev.itemcf.similarity.SimilarityMapper;
import com.eb.bi.rs.anhui.moduledev.itemcf.similarity.SimilarityReducer;

public class SimilarityDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new SimilarityDriver(), args);

        Date dateEnd = new Date();
        long timeCost = dateEnd.getTime() - dateBeg.getTime();

        PluginResult result = PluginUtil.getInstance().getResult();
        result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(ms) :" + timeCost);
        System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Logger log = PluginUtil.getInstance().getLogger();
        PluginConfig config = PluginUtil.getInstance().getConfig();
		Configuration conf;
		Job job;
		FileStatus[] status;
		long start;
		
		// 按各个品牌拆分用户评分数据（以便计算余弦相似度的分母）
		log.info("start to generate user score...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		
		String ubs = config.getParam("user_brand_score", "");
		String weightDir = config.getParam("brand_score", "");
		System.out.println("输入路径:"+ubs);
		System.out.println("输出路径:"+weightDir);
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(SimilarityDriver.class);
		check(weightDir);
		
		FileInputFormat.setInputPaths(job, new Path(ubs));
		FileOutputFormat.setOutputPath(job, new Path(weightDir));
		
		job.setMapperClass(BrandScoreMapper.class);
		job.setReducerClass(BrandScoreReducer.class);
		
		job.setNumReduceTasks(config.getParam("generate_brandscore_reduce_num", 12));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
			log.info("generate user score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate user score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
	
		// 计算品牌的相似度
		log.info("start to generate similarity matrix...");
		start = System.currentTimeMillis();
		conf = new Configuration(getConf());
		
        String bookSimDir = config.getParam("book_similarity_dir", "");
        System.out.println("输出路径:"+bookSimDir);
		check(bookSimDir);
		
		
		job = Job.getInstance(conf,"similarity");
		
		job.setJarByClass(SimilarityDriver.class);
		
		status = FileSystem.get(conf).globStatus(new Path(weightDir + "/part-*"));
		for (FileStatus st : status) {
			job.addCacheFile(st.getPath().toUri());
			log.info("cache file: " + st.getPath().toString() + " has been add into distributed cache");
		}

		FileInputFormat.setInputPaths(job, new Path(ubs));
		FileOutputFormat.setOutputPath(job, new Path(bookSimDir));
		
		job.setMapperClass(SimilarityMapper.class);
		job.setReducerClass(SimilarityReducer.class);

		job.setNumReduceTasks(config.getParam("generate_similarity_reduce_num", 12));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
			log.info("generate similarity matrix complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
		} else {
			log.error("generate similarity matrix failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
			return 1;
		}
		return 0;
	}

	private void check(String fileName) {
        Logger log = PluginUtil.getInstance().getLogger();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path path = new Path(fileName);
            boolean isExists = fs.exists(path);
            if (isExists) {
                boolean isDel = fs.delete(path, true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}
