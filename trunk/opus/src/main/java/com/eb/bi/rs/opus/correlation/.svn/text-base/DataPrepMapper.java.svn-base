package com.eb.bi.rs.opus.correlation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPrepMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		System.out.println("fields length : " + fields.length);
		if (fields.length >= 2) {
			context.write(new Text(fields[0]), new Text(fields[1]));
		}
	}
	
}
