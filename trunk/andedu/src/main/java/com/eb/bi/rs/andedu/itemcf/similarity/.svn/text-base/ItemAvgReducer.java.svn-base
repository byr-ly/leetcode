package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个物品的平均分
 */
public class ItemAvgReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		for (FloatWritable value : values) {
			count++;
			sum += value.get();
		}
		
		context.write(key, new FloatWritable(sum / count));
	}

}
