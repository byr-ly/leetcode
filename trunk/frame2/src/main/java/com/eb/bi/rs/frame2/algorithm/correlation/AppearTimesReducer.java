package com.eb.bi.rs.frame2.algorithm.correlation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AppearTimesReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Long record_num;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for(IntWritable value:values){
			count += value.get();
		}
		context.write(key,new Text(count + "|"  + record_num + "|" + count*1.0/record_num));
	}
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration configuration = context.getConfiguration();
		record_num = Long.parseLong(configuration.get("record_num"));
	}
}
