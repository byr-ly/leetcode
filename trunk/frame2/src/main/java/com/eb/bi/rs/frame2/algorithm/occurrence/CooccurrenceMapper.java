package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CooccurrenceMapper extends Mapper<Text, Text, Text, Text>{
	
	private static String m_1_separator;
	
	private static String m_2_separator;
	
	protected void setup(Context context) throws IOException,InterruptedException {
		m_1_separator = context.getConfiguration().get("id_id_separator");
		m_2_separator = context.getConfiguration().get("id_num_separator");
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] values;
		
		if(m_1_separator.equals("|")) {
			values = value.toString().split("\\|");
		}else {
			values = value.toString().split(m_1_separator);
		}
		
//		//test
//		System.out.println(value);
//		System.out.println(values.length);
//		//test
//		if(values.length > 50000) return;
		
		for(int i = 0; i != values.length; i++){
			for(int j = i; j != values.length; j++){
//				if(i==j){
//					continue;
//				}
				context.write(new Text(values[i]),new Text(values[j]+m_2_separator+1));
			}
		}
		
		values = null;
	}
	
}
