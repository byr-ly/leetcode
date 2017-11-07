package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class SourceConcatDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		Job job = Job.getInstance(conf, "source concatenate");
		job.setJarByClass(SourceConcatDriver.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setMapperClass(SourceConcatMapper.class);
		//job.setCombinerClass(SourceConcatReducer.class);
		job.setReducerClass(SourceConcatReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		String numStr = properties.getProperty("conf.num.reduce.tasks", "1");
		int numReducers = Integer.parseInt(numStr);
		job.setNumReduceTasks(numReducers);

		String inputPath = properties.getProperty("conf.input.path");
		if (inputPath != null && !"".equals(inputPath)) {
			String[] inputs = inputPath.split(",");
			for (String input : inputs) {
				MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, SourceConcatMapper.class);
			}
		}

		inputPath = properties.getProperty("conf.sequence.input.path");
		if (inputPath != null && !"".equals(inputPath)) {
			String[] inputs = inputPath.split(",");
			for (String input : inputs) {
				MultipleInputs.addInputPath(job, new Path(input), SequenceFileInputFormat.class, SourceConcatMapper.class);
			}
		}
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
