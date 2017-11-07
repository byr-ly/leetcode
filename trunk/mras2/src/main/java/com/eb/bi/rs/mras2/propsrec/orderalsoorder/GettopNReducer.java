package com.eb.bi.rs.mras2.propsrec.orderalsoorder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.eb.bi.rs.frame2.algorithm.occurrence.Items;

public class GettopNReducer extends Reducer<Text, Text, Text, Text> {
	
	//private Map<String, Items> bookordernumMaps = new HashMap<String, Items>();
	
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
		//String bookIdString = key.toString();
		
		Items bookordernumMap = new Items();
		
		bookordernumMap.setconf(m_topN,m_1_separator,m_2_separator);
		
		for (Text value : values){
			//String[] bookidInfo = value.toString().split(m_1_separator);
			
			String[] bookidInfo;
			
			if(m_1_separator.equals("|")) {
				bookidInfo = value.toString().split("\\|");
			}else {
				bookidInfo = value.toString().split(m_1_separator);
			}
			
			//context.write(key, value);
			
			bookordernumMap.add(bookidInfo[0], Float.valueOf(bookidInfo[1]));
		}
		
		//System.out.println(bookordernumMap.getitemsMap().size());
		
		bookordernumMap.generateString();
		
		context.write(key, new Text(bookordernumMap.getString(m_outWhich)));
		
		bookordernumMap.clean();

	}
	
}
