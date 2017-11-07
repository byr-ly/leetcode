package com.eb.bi.rs.frame.recframe.resultcal.offline.sorter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<CompositeKey, Text, Text, NullWritable>{
	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		int sequence = 0;
		for (Text value : values) {
			context.write(new Text(value.toString() + "|" + (++sequence)), NullWritable.get());		
		}
	}
}
