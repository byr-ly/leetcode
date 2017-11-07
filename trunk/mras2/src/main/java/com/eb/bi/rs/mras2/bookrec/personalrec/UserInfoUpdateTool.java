package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class UserInfoUpdateTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");

		Job job = new Job(conf, "usertagprefer");
		job.setJarByClass(UserInfoUpdateTool.class);
		job.setReducerClass(UserInfoUpdateReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserInfoWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);


		String historyPath = properties.getProperty("conf.history.path");
		MultipleInputs.addInputPath(job, new Path(historyPath), TextInputFormat.class, UserHistoryInfoMapper.class);
		String currentPath = properties.getProperty("conf.current.path");
		MultipleInputs.addInputPath(job, new Path(currentPath), TextInputFormat.class, UserCurrentInfoMapper.class);
		String outputPath = properties.getProperty("conf.output.path");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
			Path f = new Path(fileName);
			if (fs.exists(f)) {
				fs.delete(f, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}