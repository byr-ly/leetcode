package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author ynn 计算用户对物品评分的平均值
 *
 */
public class UserItemMeanScoreReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	/*
	 * key:用户 value:打分
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
