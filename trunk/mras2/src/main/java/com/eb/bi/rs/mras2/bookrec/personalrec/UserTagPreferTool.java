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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class UserTagPreferTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.top.n", properties.getProperty("conf.top.n", "4"));
		conf.set("conf.tag.n", properties.getProperty("conf.tag.n", "10"));
		conf.set("conf.rn.flag", properties.getProperty("conf.rn.flag", "true"));

		Job job = new Job(conf, "usertagprefer");
		job.setJarByClass(UserTagPreferTool.class);
		job.setMapperClass(UserTagPreferMapper.class);
		job.setReducerClass(UserTagPreferReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserTagPreferWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);


		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
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
