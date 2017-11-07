package com.eb.bi.rs.frame.recframe.resultcal.offline.correlationer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixSumMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] fields = value.toString().split("\\|");
		
		context.write(new Text(fields[0]+"|"+fields[1]),new Text(fields[2]));
	}
}
