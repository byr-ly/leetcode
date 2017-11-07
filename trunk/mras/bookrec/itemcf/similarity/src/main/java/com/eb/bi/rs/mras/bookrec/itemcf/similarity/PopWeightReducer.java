package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算流行图书的权重
 */
public class PopWeightReducer extends Reducer<Text, LongWritable, Text, Text> {

	Map<String, Long> popMap = new HashMap<String, Long>();

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		for (LongWritable value : values) {
			popMap.put(key.toString(), value.get());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		if (popMap.isEmpty()) {
			return;
		}

		long sum = 0;
		for (Long val : popMap.values()) {
			sum += val;
		}

		double avg = sum / (double) popMap.size();
		Text keyOut = null;
		Text valueOut = null;
		for (Map.Entry<String, Long> entry : popMap.entrySet()) {
			keyOut = new Text(entry.getKey());
			long pop = entry.getValue();
			double weight = Math.sqrt(avg / pop);
			valueOut = new Text(pop + "|" + weight);
			context.write(keyOut, valueOut);
		}
	}
}
