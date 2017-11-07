package com.eb.bi.rs.frame2.algorithm.correlation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CooccurenceMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (fields.length == 2) {
			String[] ids = fields[1].split("\\|");
			for(int i = 0; i != ids.length; i++){
				for(int j = i; j != ids.length; j++){
					context.write(new Text(ids[i]),new Text(ids[j] + "|" + 1));
				}
			}
		}
	}
	
}
