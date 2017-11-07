package com.eb.bi.rs.algorithm.occurrence;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ProportionComputeReducer extends Reducer<Text, Text, Text, Text> {
	private static String m_separator;	
		
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_separator = context.getConfiguration().get("id_num_separator");
	}
		
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String keyId = key.toString();
		
		Float keyNum = new Float(1.0);
		
		Map<String,Float> valueMap = new HashMap<String,Float>();
		
		//System.out.println(keyId+"||"+values.toString());
		
		for (Text value : values){
			String[] bookidInfo;
			if(m_separator.equals("|")) {
				bookidInfo = value.toString().split("\\|");
			}else {
				bookidInfo = value.toString().split(m_separator);
			}
			
			valueMap.put(bookidInfo[0], Float.valueOf(bookidInfo[1]));
			
			if(bookidInfo[0].equals(keyId)){
				keyNum = Float.valueOf(bookidInfo[1]);
				//break;
			}
		}
		
		Iterator<Entry<String, Float>> iterator = valueMap.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, Float> entry = iterator.next();
			
			if(keyId.equals(entry.getKey())){
				continue;
			}
			
			context.write(key, new Text(entry.getKey()+m_separator+(entry.getValue()/keyNum)));
		}
	}
}
