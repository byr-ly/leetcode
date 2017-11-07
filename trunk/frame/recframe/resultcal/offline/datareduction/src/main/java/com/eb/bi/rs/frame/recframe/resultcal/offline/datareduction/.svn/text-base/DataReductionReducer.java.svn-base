package com.eb.bi.rs.frame.recframe.resultcal.offline.datareduction;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReductionReducer extends Reducer<Text, Text, NullWritable, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {
		
		for (Text pair : values) {
			context.write(NullWritable.get(), pair);
		}
		
	}
}
