package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BookChapterMapper extends Mapper<Text, Text, Text, IntWritable>{
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		IntWritable count = new IntWritable(1);
		context.write(key,count);
	}
}