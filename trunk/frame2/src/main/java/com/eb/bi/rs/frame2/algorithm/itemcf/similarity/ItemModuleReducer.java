package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个物品的模
 */
public class ItemModuleReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		float module = 0;
		for (FloatWritable value : values){
			float val = value.get();
			module += val * val;
		}
		module = (float) Math.sqrt(module);
		context.write(key, new FloatWritable(module));
	}

}
