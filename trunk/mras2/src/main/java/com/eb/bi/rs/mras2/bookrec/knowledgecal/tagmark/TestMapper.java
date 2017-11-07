package com.eb.bi.rs.mras2.bookrec.knowledgecal.tagmark;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TestMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key,value);
	}
}


