package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author ynn 用户评分平均值
 *
 */
public class UserMeanScoreMapper extends
		Mapper<Object, Text, TextPair, Text> {

	/**
	 * value 格式：用户|平均评分
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 2) {
			context.write(new TextPair(fields[0], "0"), new Text("0|0|"+ fields[1]));	// 补齐字段
		}
	}

}
