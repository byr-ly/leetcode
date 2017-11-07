package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAreaBookTopNMapper extends Mapper<Object, Text, Text, Text>{

	private String fieldDelimiter2;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter2 = conf.get("field.delimiter.2", ";");		
	}
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(fieldDelimiter2);

		if (fields.length == 2) { // msisdn用户_专区id ; bookid图书| bookid图书
			context.write(new Text(fields[0] ), new Text( "topN|" +  fields[1] ));				
		}
	}
}
