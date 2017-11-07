package com.eb.bi.rs.andedu.combination;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class CombinationDriver extends BaseDriver {

	private static PluginUtil pluginUtil;
	private static Logger log;
	
	public CombinationDriver(String[] args) {
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}
	public static void main(String[] args) throws Exception {
		Date begin = new Date();

		int ret = ToolRunner.run(new Configuration(),
				new CombinationDriver(args), args);

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

		System.exit(0);

	}
	
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf = null;
		Job job = null;
		String inputPath2 = config.getParam("collection", "/user/recsys/pub_data/collection");
		String inputPath0 = config.getParam("visit", "/user/recsys/pub_data/visit");
		String inputPath1 = config.getParam("download", "/user/recsys/pub_data/download");
		String outputPath = config.getParam("output", "/user/recsys/job0001/user_behavior_output");
		
		conf = new Configuration(getConf());
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		job = Job.getInstance(conf, "integrated_data");
		job.setJarByClass(getClass());
		MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, CollectionMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputPath0), TextInputFormat.class, VisitMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, DownloadMapper.class);
		job.setReducerClass(IntegrationReducer.class);
		job.setNumReduceTasks(Integer.parseInt(config.getParam("reduce_num", "5")));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		check(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public void check(String path, Configuration conf) 
	{		
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}       
       
    }
}
