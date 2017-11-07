package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeFillerMapper1 extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|图书|来源|分
		String[] fields = value.toString().split("\\|");
		
		context.write(new Text(fields[0]),new Text("0|"+value.toString()));
	}
}
