package com.eb.bi.rs.mras2.unifyrec.userbooksprepare;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserReadMarkMapper extends Mapper<LongWritable, Text, Text, Text> {
	//读取用户所有的历史图书打分，格式为msisdn|bookid|book_score|adjust_score，其中book_score是图书非遗忘打分,adjust_score是图书遗忘打分
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|", 2);
		StringBuffer sb = new StringBuffer("1|");
		if(fields.length >= 2) {
			sb.append(fields[1]);
		}
		context.write(new Text(fields[0]), new Text(sb.toString()));
	}
}
