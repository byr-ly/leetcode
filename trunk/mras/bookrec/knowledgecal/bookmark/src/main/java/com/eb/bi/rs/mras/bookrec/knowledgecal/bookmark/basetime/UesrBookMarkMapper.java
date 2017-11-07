package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark.basetime;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UesrBookMarkMapper extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|图书|主要分|次要分|总分
		String[] fields = value.toString().split("\\|");
		
		context.write(new Text(fields[0]+"|"+fields[1]),new Text("0|"+value.toString()));
	}
}
