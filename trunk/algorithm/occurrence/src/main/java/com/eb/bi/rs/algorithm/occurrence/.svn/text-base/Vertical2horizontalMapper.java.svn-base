package com.eb.bi.rs.algorithm.occurrence;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Vertical2horizontalMapper extends Mapper<Text, Text, Text, Text>{
	/*
	protected void setup(Context context) throws IOException,InterruptedException {
		
	}
	*/
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key,value);
	}
	
	/*
	protected void cleanup(Context context) throws IOException ,InterruptedException{
		
	}
	*/
}
