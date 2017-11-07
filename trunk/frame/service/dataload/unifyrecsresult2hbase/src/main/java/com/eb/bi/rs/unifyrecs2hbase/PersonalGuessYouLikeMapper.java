package com.eb.bi.rs.unifyrecs2hbase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonalGuessYouLikeMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		context.write(new Text(fields[0]), new Text(fields[1]));
	}
}
