package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterHistoryMapper extends
		Mapper<LongWritable, Text, TextPair, Text> {

	/**
	 * value 格式：用户ID|动漫ID|历史评分
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		if (fields.length == 3) {
			context.write(new TextPair(fields[0] + "|" + fields[1], "0"),
					new Text("0|" + fields[2]));
		}
	}
}
