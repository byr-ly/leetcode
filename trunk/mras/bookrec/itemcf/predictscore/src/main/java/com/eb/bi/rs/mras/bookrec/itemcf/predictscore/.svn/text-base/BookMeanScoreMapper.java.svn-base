package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 计算图书平均评分
 */
public class BookMeanScoreMapper extends Mapper<Object, Text, Text, FloatWritable> {

	/**
	 * @param value 格式：用户|图书|评分
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 3) {
			return;
		}

		float score = Float.parseFloat(fields[2]);

		context.write(new Text(fields[1]), new FloatWritable(score));
	}
}
