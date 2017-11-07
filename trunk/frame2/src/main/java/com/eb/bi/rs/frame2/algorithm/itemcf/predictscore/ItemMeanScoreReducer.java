package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemMeanScoreReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	/*
	 * key:物品 value:打分
	 */
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		long count = 0;
		float scoreSum = 0;
		float scoreMean = 0;
		for (FloatWritable value : values) {
			float val = value.get();
			scoreSum += val;
			count++;
		}
		if (count > 0) {
			scoreMean = scoreSum / count;
		}
		context.write(key, new FloatWritable(scoreMean));

	}
}
