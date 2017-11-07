package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算指标的十分位数
 */
public class TenthNumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		List<Long> valList = new ArrayList<Long>();
		for (LongWritable value : values) {
			valList.add(value.get());
		}

		// 将指标从小到大排序，前90%指标中的最大值即为十分位数
		Collections.sort(valList);
		int tenth = (valList.size() * 9) / 10;
		long tenthNum = valList.get(tenth);

		context.write(new Text(key), new LongWritable(tenthNum));
	}
}
