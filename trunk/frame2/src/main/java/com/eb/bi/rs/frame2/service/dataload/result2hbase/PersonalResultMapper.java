package com.eb.bi.rs.frame2.service.dataload.result2hbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PersonalResultMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {
	
		String[] fields = value.toString().split("\\|", 2);
		if (fields.length != 2) {
			return;
		}

		context.write(new Text(fields[0]), new Text(fields[1]));
	}

}
