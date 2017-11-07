package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算指标的十分位数
 */
public class BookMeanScoreReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {


	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		long count = 0;
		float scoreSum = (float) (0.0);
		float scoreMean = (float) (0.0);
		
		for (FloatWritable value : values) {
			count++;
			scoreSum += value.get();
		}
		
		if (count>0) {
			scoreMean = scoreSum/(float)count;
		}
		
		context.write(new Text(key), new FloatWritable(scoreMean));
	}
}
