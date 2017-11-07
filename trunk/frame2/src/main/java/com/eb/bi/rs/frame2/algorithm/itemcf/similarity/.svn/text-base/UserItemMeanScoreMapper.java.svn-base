package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserItemMeanScoreMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {
	/**
	 * value 格式：用户|物品|分
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length != 3) {
			return;
		}

		float score = Float.parseFloat(fields[2]);
		context.write(new Text(fields[0]), new FloatWritable(score));
	}
}
