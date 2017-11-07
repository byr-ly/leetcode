package com.eb.bi.rs.andedu.combination;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DownloadMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		StringBuffer sb = new StringBuffer("1|");
		if(fields.length >= 3) {
			sb.append(fields[2]);
		}
		context.write(new Text(fields[0]+"|"+fields[1]), new Text(sb.toString()));
	}
}
