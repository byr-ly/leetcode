package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class PreferenceMatchDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		Job job = new Job(conf, "similarity concatenate");
		job.setJarByClass(PreferenceMatchDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(PreferenceMatchReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		String numStr = properties.getProperty("conf.num.reduce.tasks", "1");
		int numReducers = Integer.parseInt(numStr);
		job.setNumReduceTasks(numReducers);

		String prefPath = properties.getProperty("conf.pref.path");
		MultipleInputs.addInputPath(job, new Path(prefPath), TextInputFormat.class, PreferenceMapper.class);
		String srcPath = properties.getProperty("conf.src.path");
		MultipleInputs.addInputPath(job, new Path(srcPath), SequenceFileInputFormat.class, SourceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
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

	public static class SourceMapper extends Mapper<Object, Text, Text, TextPair> {
		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|", 3);
			if (fields.length != 3) {
				return;
			}
			ctx.write(new Text(fields[0] + "|" + fields[1]), new TextPair(fields[2], "1"));
		}
	}

	public static class PreferenceMapper extends Mapper<Object, Text, Text, TextPair> {
		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|");
			if (fields.length != 3) {
				return;
			}
			ctx.write(new Text(fields[0] + "|" + fields[1]), new TextPair(fields[2], "0"));
		}
	}

	public static class PreferenceMatchReducer extends Reducer<Text, TextPair, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<TextPair> values, Context ctx) throws IOException, InterruptedException {
			String src = null;
			String pref = null;
			for (TextPair value: values) {
				if ("0".equals(value.getSecond().toString())) {
					pref = value.getFirst().toString();
				} else {
					src = value.getFirst().toString();
				}
			}

			if (src != null && pref != null) {
				ctx.write(NullWritable.get(), new Text(key.toString() + "|" + pref + "|" + src));
			}
		}
	}
}
