package com.eb.bi.rs.mras2.voicebookrec.lastpage;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinBookMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	/*图书ID|系列ID|系列顺序|栏目ID*/
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		if( fields.length == 4){
			context.write(new TextPair(fields[0], "0"), new Text(fields[0]));
		}	
	}
}
