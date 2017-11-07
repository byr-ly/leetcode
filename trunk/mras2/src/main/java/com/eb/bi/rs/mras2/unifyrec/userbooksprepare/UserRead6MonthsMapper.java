package com.eb.bi.rs.mras2.unifyrec.userbooksprepare;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRead6MonthsMapper extends Mapper<LongWritable,Text, Text, Text> {
	//读取用户的近期图书列表，格式为：msisdn|book_id|1
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|", 2);
		StringBuffer sb = new StringBuffer("0|");
		if(fields.length >= 2) {
			sb.append(fields[1]);
		}
		context.write(new Text(fields[0]), new Text(sb.toString()));
	}
}
