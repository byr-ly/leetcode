package com.eb.bi.rs.mras2.unifyrec.greylist;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.mras2.unifyrec.greylist.utils.BaseDriver;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.ComponentHelper;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.JobComponent;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginConfig;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginExitCode;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginResult;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginUtil;

public class WriteGreyListDriver extends BaseDriver {

	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();

		PluginConfig pluginConfig = pluginUtil.getConfig();
		JobComponent root = ComponentHelper.createComposite(pluginConfig
				.getElement("composite"));

		Date begin = new Date();
		int ret = root.run(null);
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

		Configuration conf = new Configuration(getConf());

		// 字段分隔符
		conf.set("conf.import.split",
				properties.getProperty("conf.import.split"));

		conf.set("conf.data.size", properties.getProperty("conf.data.size"));

		// 黑名单、灰名单输出路径
		conf.set("conf.blacklist.output.path",
				properties.getProperty("conf.blacklist.output.path"));
		conf.set("conf.greylist.output.path",
				properties.getProperty("conf.greylist.output.path"));

		Job job = Job.getInstance(conf, "WriterGreyList");
		job.setJarByClass(WriteGreyListDriver.class);
		job.setMapperClass(WriteGreyListMapper.class);
		job.setReducerClass(WriteGreyListReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		String inputPath = properties.getProperty("conf.input.path");
		String outputPath = properties.getProperty("conf.output.path");
		check(outputPath, conf);

		int reduceNum = Integer.parseInt(properties.getProperty(
				"conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		MultipleOutputs.addNamedOutput(job, "blacklist", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "greylist", TextOutputFormat.class,
				Text.class, Text.class);
		
		boolean ret = job.waitForCompletion(true);
		return ret ? 0 : 1;
	}

	public void check(String path, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
