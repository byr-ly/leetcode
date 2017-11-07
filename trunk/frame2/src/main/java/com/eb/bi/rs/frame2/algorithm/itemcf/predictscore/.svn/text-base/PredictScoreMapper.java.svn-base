package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 首先以用户和目的物品为key进行分发
 */
public class PredictScoreMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * @param value
	 *            格式：用户|源物品|评分|目的物品|源物品和目的物品相似度
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 5) {
			context.write(new Text(fields[0] + "|" + fields[3]), new Text(
					fields[1] + "|" + fields[2] + "|" + fields[4]));
		}
	}
}
