package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BooktypeInputMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		//context.write(key,new Text(1+"|"+value));
		context.write(key,value);
	}
}
