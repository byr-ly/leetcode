package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserHistoryBookMapper extends Mapper<Object, Text, Text, Text>{

	private String fieldDelimiter;

	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
	}
	
	// 用户历史图书表DMN.IRECM_US_BKID_ALL  msisdn | bookid
	protected void map(Object text, Text value, Context context) throws IOException, InterruptedException{
		
		String[] fields = value.toString().split(fieldDelimiter);

		if (fields[0] != null && fields[1] != null) {
			context.write(new Text(fields[0]), new Text("his|"+fields[1]));
		}
	}
}
