package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BookInfoMapper extends Mapper<Object, Text, Text, Text> {
	
	private String fieldDelimiter;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");		
	}
	
	//bookid图书 |分类Class_id
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(fieldDelimiter);
		
		if ( fields[0] != null ) {
			context.write(new Text(fields[0]), new Text(fields[0]));
		}
	}
}
