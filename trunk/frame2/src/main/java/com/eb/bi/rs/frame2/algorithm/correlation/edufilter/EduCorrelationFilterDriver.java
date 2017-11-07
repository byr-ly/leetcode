package com.eb.bi.rs.frame2.algorithm.correlation.edufilter;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class EduCorrelationFilterDriver extends BaseDriver {
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		int reducerNum = Integer
				.parseInt(properties.getProperty("reducer.num"));
		int filterCondition = Integer.parseInt(properties
				.getProperty("filter.condition"));

		Job job = Job.getInstance(conf, getClass().getSimpleName());

		String cacheHdfsPath = properties.getProperty("hdfs.cache.path");
		if (cacheHdfsPath == null) {
			throw new RuntimeException("cache hdfs path is essential");
		}
		conf.set("hdfs.cache.path", cacheHdfsPath);
		conf.setInt("filter.condition", filterCondition);
		FileSystem fs = FileSystem.get(URI.create(cacheHdfsPath), conf);
		FileStatus[] status = fs.listStatus(new Path(cacheHdfsPath));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
		}
		String inputPath = properties.getProperty("hdfs.input.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}

		String outputPath = properties.getProperty("hdfs.output.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("output path is essential");
		}

		job.setJarByClass(getClass());
		job.setNumReduceTasks(reducerNum);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setMapperClass(EduCorrelationFilterMapper.class);
		job.setReducerClass(EduCorrelationFilterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		check(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : -1;
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
