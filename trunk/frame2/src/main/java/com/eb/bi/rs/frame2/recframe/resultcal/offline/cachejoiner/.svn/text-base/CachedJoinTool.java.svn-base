package com.eb.bi.rs.frame2.recframe.resultcal.offline.cachejoiner;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class CachedJoinTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		String outputDelimiter = properties.getProperty("conf.output.delimiter", "|");
		conf.set("mapred.textoutputformat.separator", outputDelimiter);
		conf.set("conf.output.delimiter", outputDelimiter);
		String inputDelimiter = properties.getProperty("conf.input.delimiter", "\\|");
		conf.set("conf.input.delimiter", inputDelimiter);
		String isInputFirst = properties.getProperty("conf.output.input.first", "true");
		conf.set("conf.output.input.first", isInputFirst);
		String cacheDelimiter = properties.getProperty("conf.cache.delimiter", "\\|");
		conf.set("conf.cache.delimiter", cacheDelimiter);
		String cacheKeyIndex = properties.getProperty("conf.cache.key.index");
		conf.set("conf.cache.key.index", cacheKeyIndex);
		String inputKeyIndex = properties.getProperty("conf.input.key.index");
		conf.set("conf.input.key.index", inputKeyIndex);
		
		String outputOrderDelimiter = properties.getProperty("conf.output.order.delimiter", ",");
		if (outputOrderDelimiter != null) {
			conf.set("conf.output.order.delimiter", outputOrderDelimiter);
		}
		String outputOrder = properties.getProperty("conf.output.order");
		if (outputOrder != null) {
			conf.set("conf.output.order", outputOrder);
		}

		String inputFormatClass = properties.getProperty("conf.inputformat.class");
		if (inputFormatClass != null) {
			conf.setClass("mapreduce.inputformat.class", Class.forName(inputFormatClass), InputFormat.class);
		}
		
		String outputFormatClass = properties.getProperty("conf.outputformat.class");
		if (outputFormatClass != null) {
			conf.setClass("mapreduce.outputformat.class", Class.forName(outputFormatClass), OutputFormat.class);
		}
		
		String isOutputCompress = properties.getProperty("conf.output.compress");
		if (isOutputCompress != null) {
			conf.setBoolean("mapred.output.compress", Boolean.parseBoolean(isOutputCompress));
		}

		String outputCodec = properties.getProperty("conf.output.compression.codec");
		if (outputCodec != null) {
			conf.setClass("mapred.output.compression.codec", Class.forName(outputCodec), CompressionCodec.class);
		}

		String outputCompType = properties.getProperty("conf.output.compression.type");
		if (outputCompType != null) {
			conf.set("mapred.output.compression.type", outputCompType);
		}

		String cachePath = properties.getProperty("conf.cache.path");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(cachePath);
		Job job = Job.getInstance(conf, "cachedjoin");
		FileStatus[] status = fs.globStatus(path);
		for (FileStatus st : status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}

		job.setJarByClass(CachedJoinTool.class);
		job.setMapperClass(CachedJoinMapper.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
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
