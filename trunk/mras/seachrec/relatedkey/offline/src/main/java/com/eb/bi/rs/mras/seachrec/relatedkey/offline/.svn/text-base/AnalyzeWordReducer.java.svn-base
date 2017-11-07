package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AnalyzeWordReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		for(IntWritable value:values){
			count += Integer.parseInt(value.toString());
		}
		
		context.write(key,new IntWritable(count));
	}
}
