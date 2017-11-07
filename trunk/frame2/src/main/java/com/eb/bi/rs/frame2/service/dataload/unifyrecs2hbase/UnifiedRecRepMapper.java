package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnifiedRecRepMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|", -1);
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i < fields.length; i++)
			sb.append(fields[i] + "|");
		if (sb.length() > 0)
			sb.deleteCharAt(sb.length() - 1);
		context.write(new Text(fields[0]), new Text(sb.toString()));

	}
}
