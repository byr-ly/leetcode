package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class SimilarityConcatDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		String dimStr = properties.getProperty("conf.similarity.dimension", "2");
		conf.set("conf.similarity.dimension", dimStr);

		Job job = new Job(conf, "similarity concatenate");
		job.setJarByClass(SimilarityConcatDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SimilarityConcatMapper.class);
		job.setReducerClass(SimilarityConcatReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		String numStr = properties.getProperty("conf.num.reduce.tasks", "1");
		int numReducers = Integer.parseInt(numStr);
		job.setNumReduceTasks(numReducers);

		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, inputPath);
		String outputPath = properties.getProperty("conf.output.path");
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);

		return job.waitForCompletion(true) ? 0: 1;
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
