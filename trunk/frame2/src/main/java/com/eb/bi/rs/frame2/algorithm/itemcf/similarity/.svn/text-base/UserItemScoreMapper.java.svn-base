package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author ynn 用户物品打分
 *
 */
public class UserItemScoreMapper extends
		Mapper<Object, Text, TextPair, Text> {

	/**
	 * value 格式：用户|物品|评分
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 3) {
			context.write(new TextPair(fields[0], "1"), new Text("1|"+ fields[1] + "|" + fields[2]));
		}
	}

}
