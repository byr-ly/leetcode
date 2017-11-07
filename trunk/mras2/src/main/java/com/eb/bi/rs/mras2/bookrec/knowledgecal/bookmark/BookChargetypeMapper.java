package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn 
 * @date 创建时间：2015-12-25 下午3:35:21
 * @version 1.0
 */
public class BookChargetypeMapper extends Mapper<Text, Text, Text, Text>{

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, new Text(1 + "|" + value));
	}
}
