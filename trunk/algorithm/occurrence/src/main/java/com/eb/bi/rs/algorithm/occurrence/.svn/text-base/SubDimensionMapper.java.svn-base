package com.eb.bi.rs.algorithm.occurrence;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SubDimensionMapper extends Mapper<Text, Text, Text, Text>{
	private Set<String> itemId = new HashSet<String>();
	
	private String m_1_separator;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_1_separator = context.getConfiguration().get("id_id_separator");
		
		//过滤列表加载
		
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] values = value.toString().split(m_1_separator);
		
		if(itemId.contains(key.toString())||itemId.contains(values[0])){
			return;
		}
		
		context.write(key,value);
	}
}
