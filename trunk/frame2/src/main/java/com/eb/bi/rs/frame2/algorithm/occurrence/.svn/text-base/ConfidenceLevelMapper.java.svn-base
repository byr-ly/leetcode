package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ConfidenceLevelMapper extends Mapper<Text, Text, Text, Text>{
	private float m_min_num;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_min_num = Float.valueOf(context.getConfiguration().get("min_num"));
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] bookidInfo;
		
		bookidInfo = value.toString().split("\\|");
		
		if(bookidInfo.length != 2)
			return;
		
		if(bookidInfo[0].equals(key.toString()))
			return;
		
		if(m_min_num > 0)
			if(Float.valueOf(bookidInfo[1]) < m_min_num)
				return;
		
		context.write(key,value);
		
		//context.write(new Text(bookidInfo[0]),new Text(key.toString() + "|" + bookidInfo[1]));
	}
}
