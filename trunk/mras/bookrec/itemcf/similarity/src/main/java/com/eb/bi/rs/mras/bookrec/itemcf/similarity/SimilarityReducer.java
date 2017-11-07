package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算两本图书之间的相似度
 */
public class SimilarityReducer extends Reducer<Text, SimilarityWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<SimilarityWritable> values, Context context) throws IOException, InterruptedException {
		int cnt  = 0;
		double sumW = 0.0;
		double sumI = 0.0;
		double sumJ = 0.0;
		for (SimilarityWritable value : values) {
			cnt++;
			sumW += value.getWr();
			sumI += value.getRi();
			sumJ += value.getRj();
		}
		
		double deno = Math.sqrt(sumI) * Math.sqrt(sumJ);
		if (sumW > 0.0 && deno > 0.0) {
			double sim = sumW / deno;
			context.write(key, new Text(String.format("%f|%d", sim, cnt)));
		}
	}
}

