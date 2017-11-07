package com.eb.bi.rs.frame2.algorithm.correlation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateIndexMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\t");
		if (fields.length != 2) {
			return;
		}
		String[] items = fields[1].split("\\|");
		if (items.length != 2) {
			return;
		}
		if(fields[0].equals(items[0]))
			return;
		
		context.write(new Text(fields[0]),new Text(fields[1]));
	}
	
}
