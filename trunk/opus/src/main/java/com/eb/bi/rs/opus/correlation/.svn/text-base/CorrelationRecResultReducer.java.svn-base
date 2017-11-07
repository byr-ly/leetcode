package com.eb.bi.rs.opus.correlation;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CorrelationRecResultReducer extends Reducer<Text, Text, Text, Text> {

	private int recNum;

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		Map<String , Double> resultMap = new HashMap<String , Double>();
		for (Text text : values) {
			String[] fields = text.toString().split("\\|");
			if (fields.length == 2) {
				resultMap.put(fields[0], Double.parseDouble(fields[1]));
			}
		}
		String result = sort(resultMap);
		
		context.write(key, new Text(result));
	}
	
	private String sort(Map<String , Double> map) {
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>( map.entrySet() );  
        Collections.sort( list, new Comparator<Map.Entry<String, Double>>()  
        {  
            public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )  
            {  
                return (o2.getValue()).compareTo( o1.getValue() );  
            }  
        } );  
  
        StringBuffer results = new StringBuffer();
        int n = list.size() < recNum ? list.size():recNum;
        for (int i = 0; i < n; i++) {
        	results.append(list.get(i).getKey()+","+list.get(i).getValue()+"|");
		} 
		return results.toString();
	}
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		recNum = Integer.parseInt(conf.get("rec.num"));
	}
}
