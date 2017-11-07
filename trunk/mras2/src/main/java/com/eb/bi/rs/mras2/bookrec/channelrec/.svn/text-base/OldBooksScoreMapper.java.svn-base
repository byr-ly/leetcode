package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OldBooksScoreMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	//读取用户所有的历史图书打分，格式为msisdn	bookid,book_score|bookid,book_score|...其中book_score是图书非遗忘打分
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t", 2);
		StringBuffer sb = new StringBuffer("1|");
		if(fields.length >= 2) {
			sb.append(fields[1]);
		}
		context.write(new Text(fields[0]), new Text(sb.toString()));
	}
}
