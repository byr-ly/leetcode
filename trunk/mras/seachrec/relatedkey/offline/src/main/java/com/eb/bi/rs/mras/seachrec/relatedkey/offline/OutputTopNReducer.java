package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class OutputTopNReducer extends Reducer<Text, Text, Text, Text> {
	private static String m_1_separator;	
	//private static String m_2_separator;	
	private static Integer m_topN;	
		
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_topN = Integer.valueOf(context.getConfiguration().get("top_num"));
		m_1_separator = context.getConfiguration().get("id_num_separator");
		//m_2_separator = context.getConfiguration().get("id_id_separator");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Map<String,Float> booknumMap = new TreeMap<String, Float>();
		//每次保留topN
		for (Text value : values){
			String[] bookidInfo;
			if(m_1_separator.equals("|")) {
				bookidInfo = value.toString().split("\\|");
			}else {
				bookidInfo = value.toString().split(m_1_separator);
			}			
			if(bookidInfo[0].equals(key.toString())){
				continue;
			}
			
			booknumMap.put(bookidInfo[0], Float.valueOf(bookidInfo[1]));
			
			if(booknumMap.size() > m_topN){
				List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>( 
						booknumMap.entrySet());
				
				//排序
				Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>(){
					public int compare(Map.Entry<String, Float> o1,
					Map.Entry<String, Float> o2){
					return ((int)(o2.getValue() - o1.getValue()));
					}
					});
				
				booknumMap.remove(infoIds.get(infoIds.size()-1).getKey());
			}
		}
		
		//输出结果
		List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>( 
				booknumMap.entrySet());
		
		//排序
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>(){
			public int compare(Map.Entry<String, Float> o1,
			Map.Entry<String, Float> o2){
			return ((int)(o2.getValue() - o1.getValue()));
			}
			});
		
		for(int i = 0; i != infoIds.size(); i++){
			String result = infoIds.get(i).getKey() + "|" + infoIds.get(i).getValue().toString();
			
			context.write(key,new Text(result));
		}
	}
}
