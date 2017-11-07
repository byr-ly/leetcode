package com.eb.bi.rs.algorithm.occurrence;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class OutputMatrixReducer extends Reducer<Text, Text, Text, Text> {
	private static String m_1_separator;	
	private static String m_2_separator;	
	private static Integer m_topN;	
	private static int m_outWhich;
		
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_outWhich = Integer.valueOf(context.getConfiguration().get("out_Which")).intValue();
		m_topN = Integer.valueOf(context.getConfiguration().get("top_num"));
		m_1_separator = context.getConfiguration().get("id_num_separator");
		m_2_separator = context.getConfiguration().get("id_id_separator");
	}
		
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Items bookordernumMap = new Items();
		
		
		
		//输出配置项
		bookordernumMap.setconf(m_topN,m_1_separator,m_2_separator);
		
		for (Text value : values){
			//String[] bookidInfo = value.toString().split(m_1_separator);
			String[] bookidInfo;
			if(m_1_separator.equals("|")) {
				bookidInfo = value.toString().split("\\|");
			}else {
				bookidInfo = value.toString().split(m_1_separator);
			}			
			if(bookidInfo[0].equals(key.toString())){
				continue;
			}
			
			bookordernumMap.add(bookidInfo[0], Float.valueOf(bookidInfo[1]));
		}
		
		//输出字符串生成
		bookordernumMap.generateString();
		
		String valueString = bookordernumMap.getString(m_outWhich);
		if(valueString.equals("")){
			return;
		}
		
		context.write(key, new Text(valueString));
		
		bookordernumMap.clean();
	}	
}
