package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn
 * 
 */
public class CollectMatrixMappper extends
		Mapper<LongWritable, Text, Text, Text> {

	/*
	 * 格式：IMEI用户ID|动漫ID|第一次收藏时间
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length >= 3) {
			// key : 用户ID|动漫ID value : 1|1 ，第一个1表示收藏行为，第二个1表示次数
			if (!"".equals(fields[0]) && !"".equals(fields[1])) {
				context.write(new Text(fields[0] + "|" + fields[1]), new Text(
						"1|" + fields[2]));
			}
		}
	}
}
