package com.eb.bi.rs.mras.propsrec.orderalsoorder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GettopNMapper extends Mapper<Text, Text, Text, Text>{
	
	//private static String m_1_separator;
	
	private static String m_2_separator;
	
	protected void setup(Context context) throws IOException,InterruptedException {
		//m_1_separator = context.getConfiguration().get("id_id_separator");
		m_2_separator = context.getConfiguration().get("id_num_separator");
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] bookidInfo;
		
		if(m_2_separator.equals("|")) {
			bookidInfo = value.toString().split("\\|");
		}else {
			bookidInfo = value.toString().split(m_2_separator);
		}
		
		if(bookidInfo[0].equals(key.toString())){
			return;
		}
		
		//context.write(new Text(values[0]), new Text(key.toString()+m_2_separator+values[1]));		
		context.write(key,value);
	}
	
}
