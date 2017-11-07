package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBookPrefersocreMapper extends Mapper<Object, Text, Text, Text> {

	private String fieldDelimiter2;

	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		fieldDelimiter2 = conf.get("field.delimiter.2", ";");
	}
	// msisdn ; bookid , prefer_socre 偏好分 | bookid , prefer_socre 偏好分
	protected void map(Object text, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(fieldDelimiter2);

		if ( fields.length == 2 && fields[0] != null && fields[1] != null ) {
			context.write(new Text(fields[0]), new Text("pre|"+fields[1]));
		}
	}
}
