package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeFillerMapper2 extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|图书
		String[] fields = value.toString().split("\\|");
		
		context.write(new Text(fields[0]),new Text("1|"+value.toString()));
	}
}
