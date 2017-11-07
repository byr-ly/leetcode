package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HotWordListReducer extends Reducer<Text,Text, Text, Text> {
	private Map<String,Integer> hotwordList = new TreeMap<String,Integer>();
	private int topN;
	private String m_recordday;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		topN = Integer.valueOf(context.getConfiguration().get("hot_top_num")).intValue();
		m_recordday = context.getConfiguration().get("record_day");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text value:values){
			String sentence = value.toString();	
			hotwordList.put(key.toString(),Integer.valueOf(sentence));
			
			if(hotwordList.size() > topN){
				List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>( 
						hotwordList.entrySet());
				//排序
				Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>(){
					public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2){
					return ((int)(o2.getValue() - o1.getValue()));
					}
					});
				hotwordList.remove(infoIds.get(infoIds.size()-1).getKey());
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException 
	{
		Iterator<Entry<String, Integer>> iterator = hotwordList.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, Integer> entry = iterator.next();
			 
			String value = entry.getValue().toString() + "|" + m_recordday;
			 
			context.write(new Text(entry.getKey()),new Text(value));
		 }
		super.cleanup(context);
		
	}
}
