package com.eb.bi.rs.mras2.unifyrec.userbooksprepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class UserRead6MonthsTool extends BaseDriver {

	public static void rmr(String folder, Configuration conf)
			throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(conf);
		fs.deleteOnExit(path);
		fs.close();
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		Job job = new Job(conf);
		job.setJarByClass(UserRead6MonthsTool.class);
		job.setJobName("UserBooksPrepare");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(UserRead6MonthsReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int reduceNum = Integer.parseInt(properties.getProperty(
				"conf.num.reduce.tasks", "10"));
		job.setNumReduceTasks(reduceNum);

		String sixcmInputPath = properties.getProperty("conf.input.6cm.path");
		String scoreInputPath = properties.getProperty("conf.input.score.path");
		MultipleInputs.addInputPath(job, new Path(sixcmInputPath),
				TextInputFormat.class, UserRead6MonthsMapper.class);
		MultipleInputs.addInputPath(job, new Path(scoreInputPath),
				TextInputFormat.class, UserReadMarkMapper.class);

		MultipleOutputs.addNamedOutput(job, "recentBook",
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "bookMark", TextOutputFormat.class,
				Text.class, Text.class);
		String outputPath = properties.getProperty("conf.output.path");
		
		//在执行新的job之前先把上一次的输出目录进行删除
		rmr(outputPath,conf);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
}

/*
 * public class UserRead6MonthsTool extends Configured implements Tool {
 * 
 * public int run(String[] args) throws Exception { Configuration conf = new
 * Configuration(); Job job = new Job(conf);
 * job.setJarByClass(UserRead6MonthsTool.class);
 * job.setJobName("UserBookPrepare");
 * 
 * job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class);
 * 
 * job.setReducerClass(UserRead6MonthsReducer.class);
 * 
 * job.setInputFormatClass(TextInputFormat.class);
 * job.setOutputFormatClass(TextOutputFormat.class);
 * 
 * job.setNumReduceTasks(10);
 * 
 * MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
 * UserRead6MonthsMapper.class); MultipleInputs.addInputPath(job, new
 * Path(args[1]), TextInputFormat.class, UserReadMarkMapper.class);
 * 
 * MultipleOutputs.addNamedOutput(job, "recentBook",
 * TextOutputFormat.class,Text.class, Text.class);
 * MultipleOutputs.addNamedOutput(job, "bookMark",
 * TextOutputFormat.class,Text.class, Text.class);
 * FileOutputFormat.setOutputPath(job, new Path(args[2]));
 * 
 * boolean success = job.waitForCompletion(true); return success ? 0 : 1;
 * 
 * }
 * 
 * public static void main(String[] args) throws Exception { int ret =
 * ToolRunner.run(new UserRead6MonthsTool(), args); System.exit(ret); }
 * 
 * }
 */