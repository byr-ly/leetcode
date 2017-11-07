package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算用户topN评分的图书
 */
public class TopUserBooksMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * @param value 格式：用户|图书|评分
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 3) {
			return;
		}

		context.write(new Text(fields[0]), new Text(fields[1] + "|" + fields[2]));
	}
}
