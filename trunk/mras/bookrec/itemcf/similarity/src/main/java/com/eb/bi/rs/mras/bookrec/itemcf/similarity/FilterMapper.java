package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 过滤掉阅读用户数、订购用户数、深度阅读用户数任意一项指标为0的图书
 */
public class FilterMapper extends Mapper<Object, Text, Text, NullWritable> {

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
		if (bookUcAll == 0) {
			return;
		}

		long orderUc = Long.parseLong(fields[2]);
		if (orderUc == 0) {
			return;
		}

		long bookUc = Long.parseLong(fields[3]);
		if (bookUc == 0) {
			return;
		}

		context.write(value, NullWritable.get());
	}
}
