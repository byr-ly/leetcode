package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 提取流行图书的订购用户数
 */
public class PopWeightMapper extends Mapper<Object, Text, Text, LongWritable> {

	/**
	 * @param value 格式：图书|阅读用户数|订购用户数|深度阅读用户数
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 4) {
			return;
		}

		String bookId = fields[0];
		long orderUc = Long.parseLong(fields[2]);

		context.write(new Text(bookId), new LongWritable(orderUc));
	}
}
