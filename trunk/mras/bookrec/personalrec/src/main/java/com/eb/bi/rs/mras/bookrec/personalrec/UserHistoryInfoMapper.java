package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserHistoryInfoMapper extends Mapper<Object, Text, Text, UserInfoWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|", 2);
		if (fields.length != 2) {
			return;
		}
		context.write(new Text(fields[0]), new UserInfoWritable(UserInfoWritable.HISTORY, fields[1]));
	}
}
