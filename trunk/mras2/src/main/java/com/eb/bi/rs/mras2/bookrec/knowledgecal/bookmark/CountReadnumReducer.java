package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author ynn 
 * @date 创建时间：2015-12-25 下午4:10:19
 * @version 1.0
 */
public class CountReadnumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException ,InterruptedException {
		int count = 0;
		for(IntWritable value: values){
			count += Integer.parseInt(value.toString());
		}
		context.write(key, new IntWritable(count));
	}
}
