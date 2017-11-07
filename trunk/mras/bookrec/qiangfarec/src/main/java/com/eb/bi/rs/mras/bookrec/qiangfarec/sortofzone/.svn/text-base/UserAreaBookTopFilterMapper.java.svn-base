package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAreaBookTopFilterMapper extends Mapper<Object, Text, Text, Text>{

	private String fieldDelimiter;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");	
	}
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(fieldDelimiter);

		if (fields.length == 3) { // msisdn|专区ID|bookid1 ,bookid2 ,book3,...
			context.write(new Text(fields[0] +"_" + fields[1] ), new Text( "result|"+fields[2] ));				
		}
	}
}
