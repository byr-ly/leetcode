package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterPredictHistoryReducer extends
		Reducer<TextPair, Text, Text, NullWritable> {

	/*
	 * key:用户ID		value:0/1|动漫ID|评分
	 */
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 过滤用户动漫历史
		for (Text value : values) {
			String[] fields = value.toString().split("\\|", -1);
			if (fields[0].equals("0")) {
				return;
			}

			if (fields[0].equals("1")) {
				context.write(new Text(key.getFirst() + "|" + fields[1]),
						NullWritable.get());
			}
		}
	}
}
