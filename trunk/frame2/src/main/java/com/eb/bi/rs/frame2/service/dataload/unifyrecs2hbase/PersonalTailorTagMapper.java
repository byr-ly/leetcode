package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonalTailorTagMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");

		int len = fields.length;
		for (int i = 2; i < len; i++) {
			fields[i] = fields[i] + "," + fields[1];
			context.write(new Text(fields[0]), new Text(fields[i]));
		}
	}
}
