package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterBooksMapper extends Mapper<Object, Text, Text, Text>{
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		StringBuffer sb = new StringBuffer("3|");
		if(fields.length >= 2) {
			sb.append(fields[1]);
		}
		context.write(new Text(fields[0]), new Text(sb.toString()));
	}
}
