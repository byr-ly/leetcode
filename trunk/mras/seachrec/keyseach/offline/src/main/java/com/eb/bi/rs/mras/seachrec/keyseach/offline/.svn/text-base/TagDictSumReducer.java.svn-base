package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TagDictSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{	
	
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException ,InterruptedException {
		int sum = 0;	

		for(IntWritable value: values){
			sum += value.get();		
		}
		
		context.write(key, new IntWritable(sum));
	}
}
