package com.eb.bi.rs.mras.seachrec.keyseach.offline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class AggregateKeyWordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> multipleOutputs;
	private int reservedLowFrequencyKeywordValue = 2;

	protected void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException ,InterruptedException {
		int count = 0;
		for(IntWritable value:values){
			count += Integer.parseInt(value.toString());
		}
		multipleOutputs.write(key, new IntWritable(count), "total/part");	
		if(count >= reservedLowFrequencyKeywordValue){
			multipleOutputs.write(key, new IntWritable(count), "filtered/part");
		}

	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException 
	{
		super.setup(context);		
		reservedLowFrequencyKeywordValue = context.getConfiguration().getInt("reserved.low.frequency.keyword.value", 2);
		multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);

	}
	
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException 
	{
		multipleOutputs.close();
		super.cleanup(context);
		
	}


}

