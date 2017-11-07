package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 将图书信息按指标分组
 */
public class TenthNumMapper extends Mapper<Object, Text, Text, LongWritable> {

	/**
	 * @param value 格式：图书|阅读用户数|订购用户数|深度阅读用户数
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 4) {
			return;
		}

		long bookUcAll = Long.parseLong(fields[1]);
		long orderUc = Long.parseLong(fields[2]);
		long bookUc = Long.parseLong(fields[3]);

		context.write(new Text("book_uc_all"), new LongWritable(bookUcAll));
		context.write(new Text("order_uc"), new LongWritable(orderUc));
		context.write(new Text("book_uc"), new LongWritable(bookUc));
	}
}
