package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NoFillerUserFilterMapper 
    extends Mapper<Object, Text, Text, IntWritable>
{
	
	/**
	 * @param value:
	 *     输入：
	 *     格式：msisdn|authorid|bookid|score|type
	 * map out:
	 */
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		String[] strs = value.toString().split("\\|");
		String msisdn = strs[0];
		context.write(new Text(msisdn), new IntWritable(1));
	}
	
}
