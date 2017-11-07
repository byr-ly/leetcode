package com.eb.bi.rs.opus.hdfs2hbase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class ImportDataProTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());

		// 数据分隔符
		conf.set("conf.data.split", properties.getProperty("conf.data.split"));
		// rowkey索引值，用逗号分隔
		conf.set("conf.rowkey.indexs", properties.getProperty("conf.rowkey.indexs"));
		// 输出rowkey分隔符
		conf.set("conf.rowkey.split", properties.getProperty("conf.rowkey.split"));
		// 输出rowkey和data之间的分隔符
		conf.set("conf.rowkey.data.split", properties.getProperty("conf.rowkey.data.split"));
		
		Job job = Job.getInstance(conf, "ImportDataPro");
		job.setNumReduceTasks(0);
		job.setJarByClass(ImportDataProTool.class);
		job.setMapperClass(ImportDataProMapper.class);
		job.setReducerClass(ImportDataProReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		String outputPath = properties.getProperty("conf.output.path");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);

		boolean ret = job.waitForCompletion(true);
		return ret ? 0 : 1;
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
