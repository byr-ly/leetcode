package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn 
 * @date 创建时间：2015-12-25 下午3:09:23
 * @version 1.0
 */
public class BookChapterMapper extends Mapper<Text, Text, Text, IntWritable>{

	@Override
	protected void map(Text key, Text value, Context context) throws IOException ,InterruptedException {
		IntWritable count = new IntWritable(1);
		context.write(key, count);
	}
}
