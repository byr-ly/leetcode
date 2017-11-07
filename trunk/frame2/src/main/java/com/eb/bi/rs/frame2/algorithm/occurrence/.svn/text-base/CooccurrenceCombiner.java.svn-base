package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CooccurrenceCombiner extends Reducer<Text, Text, Text, Text> {
	
	//private Map<String, Items> bookordernumMaps = new HashMap<String, Items>();
	
	private static String m_1_separator;
	
	//private static String m_2_separator;
	
	//private static Integer m_topN;
	
	//private static int m_outWhich;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		//m_outWhich = Integer.valueOf(context.getConfiguration().get("out_Which")).intValue();
		
		//m_topN = Integer.valueOf(context.getConfiguration().get("top_num"));
		
		m_1_separator = context.getConfiguration().get("id_num_separator");
		
		//m_2_separator = context.getConfiguration().get("id_id_separator");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//String bookIdString = key.toString();
		
		Items bookordernumMap = new Items();
		
		//bookordernumMap.setconf(m_topN,m_1_separator,m_2_separator);
		
		for (Text value : values){
			String[] bookidInfo;
			if(m_1_separator.equals("|")) {
				bookidInfo = value.toString().split("\\|");
			}else {
				bookidInfo = value.toString().split(m_1_separator);
			}
			
			if(bookidInfo.length != 2)
				continue;
				
			//context.write(key, value);

			bookordernumMap.add(bookidInfo[0], Float.valueOf(bookidInfo[1]));
		}
		
		//System.out.println(bookordernumMap.getitemsMap().size());
		
		Iterator<Entry<String, Float>> iterator = bookordernumMap.getitemsMap().entrySet().iterator();
		while(iterator.hasNext()){
			 Entry<String, Float> entry = iterator.next();
			 context.write(key, new Text(entry.getKey()+m_1_separator+entry.getValue().toString()));
		}
		
		bookordernumMap.clean();
		
		bookordernumMap = null;
		
		//bookordernumMap.clean();context.write(key, new Text(entry.getKey()+m_1_separator+entry.getValue()));
		
		/*
		if(bookordernumMaps.containsKey(bookIdString)){
			bookordernumMaps.get(bookIdString).addAll(bookordernumMap.getitemsMap());
			//bookordernumMaps.put(bookIdString, bookordernumMap);
		}
		else {
			bookordernumMaps.put(bookIdString, bookordernumMap);
		}
		*/
	}
	
	/*
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException{
		Iterator<Entry<String, Items>> iterator = bookordernumMaps.entrySet().iterator();
		 while(iterator.hasNext()){
			 Entry<String, Items> entry = iterator.next();
			 entry.getValue().change2listString();
			 for(int i = 0;i != entry.getValue().getitemsMap().size();i++){
			 
			 context.write(new Text(entry.getKey()), new Text(entry.getValue().getpairString(i)));
			 }
		 }
	}
	*/
}
