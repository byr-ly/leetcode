package com.eb.bi.rs.frame2.recframe.base;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class SplitMapper extends Mapper<Object, Text, Text, IntWritable> {
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokens = new StringTokenizer(line);
		while (tokens.hasMoreTokens()) {
			String word = tokens.nextToken();
			context.write(new Text(word), new IntWritable(1));
		}
	}
}

class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	};
}

public class DriverExample extends BaseDriver{
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String input = properties.getProperty("input.path", "wordcount/intput");
		String output = properties.getProperty("output.path", "wordcount/output");
		// hadoop job
		Job job = Job.getInstance(conf);
		job.setJarByClass(DriverExample.class);
		job.setMapperClass(SplitMapper.class);
		//job.setCombinerClass(CountReduce.class);
		job.setReducerClass(CountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
