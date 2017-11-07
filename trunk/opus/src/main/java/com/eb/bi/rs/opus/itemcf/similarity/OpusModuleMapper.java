package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OpusModuleMapper extends Mapper<Object, Text, Text, FloatWritable> {
	
	/**
	 * value 格式 ：用户|动漫|评分
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if(fields.length == 3){
			String opus = fields[1];
			float score = Float.parseFloat(fields[2]);
			context.write(new Text(opus), new FloatWritable(score));
		}
		
	}

}
