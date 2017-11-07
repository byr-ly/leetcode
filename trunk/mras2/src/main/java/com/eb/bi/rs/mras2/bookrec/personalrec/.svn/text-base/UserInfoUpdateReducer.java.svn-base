package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class UserInfoUpdateReducer extends Reducer<Text, UserInfoWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<UserInfoWritable> values, Context context) throws IOException, InterruptedException {
		String info = null;
		for (UserInfoWritable value : values) {
			info = value.info;
			if (UserInfoWritable.CURRENT == value.flag) {
				break;
			}
		}

		context.write(new Text(key), new Text(info));
	}
}
