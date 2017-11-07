package com.eb.bi.rs.mras.bookrec.personalrec.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewBookFillerMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {
		//bookid图书|bu_type事业部|class_id分类|vst_uv访问用户
		String[] fields = value.toString().split("\\|");
		
		//分类为key发出去
		context.write(new Text(fields[2]), value);
	}
}
