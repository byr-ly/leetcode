package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Float;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算用户topN评分的图书
 */
public class TopUserBooksReducer extends Reducer<Text, Text, Text, FloatWritable> {
	private int topN;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Float> simMap = new HashMap<String, Float>();		
		for (Text value : values) {
			String field[] = value.toString().split("\\|");
			simMap.put(field[0], Float.parseFloat(field[1]));
		}
		
    	List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>(simMap.entrySet());
    	// 根据value值从大到小排序
    	Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>() {   
    		public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {      
    		return (o2.getValue()).compareTo(o1.getValue()); 
    		}
    	});
    	
		int count = 0;
		for (int i = 0; i<infoIds.size() && count<topN; i++) {
			context.write(new Text(key.toString() + "|" + infoIds.get(i).getKey()), new FloatWritable(infoIds.get(i).getValue()));
			count++;
		}
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		topN =  Integer.parseInt(conf.get("conf.top.userbookscore.num", "20"));
	}
}
