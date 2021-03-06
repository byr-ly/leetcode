package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FrequencyReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		for(IntWritable value:values){
			count += Integer.parseInt(value.toString());
		}
		
		context.write(key,new IntWritable(count));
	}
}
