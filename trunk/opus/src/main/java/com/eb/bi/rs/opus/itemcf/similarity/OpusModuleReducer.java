package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个动漫的模
 * @author lenovo
 *
 */
public class OpusModuleReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		float module = 0;
		for(FloatWritable value: values){
			float val = value.get();
			module += val * val;
		}
		module = (float) Math.sqrt(module);
		context.write(key, new FloatWritable(module));
	}
}
