package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderedTopNOrderByMultiFieldCombiner extends Reducer<Text, Text, Text, Text>{
	private String fieldDelimiter;	
	private int topNNumber;
	private HashMap<Integer, String> orderMode = new HashMap<Integer, String>();/*2:asc,6:asc*/
	@Override
	protected void reduce(Text key,Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		TreeSet<String> set = new TreeSet<String>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				String[] fields1 ;
				if(fieldDelimiter.equals("|")){
					fields1 = o1.split("\\|",-1);
				}else {
					fields1 = o1.split(fieldDelimiter);
				}
				String[] fields2;
				if(fieldDelimiter.equals("|")){
					fields2 = o2.split("\\|",-1);
				}else {
					fields2 = o2.split(fieldDelimiter);
				}				
				Iterator<Entry<Integer, String>> iter = orderMode.entrySet().iterator();
				while(iter.hasNext()){	
					Entry<Integer, String> entry = iter.next();
					double orderByField1 = Double.parseDouble(fields1[entry.getKey()]);
					double orderByField2 = Double.parseDouble(fields2[entry.getKey()]);
					if( entry.getValue().equalsIgnoreCase("asc")){
						if(orderByField1 > orderByField2) {
							return 1;
						}else if (orderByField1 < orderByField2) {
							return -1;
						}						
					}else {
						if( orderByField2 > orderByField1 ){
							return 1;
						}else if (orderByField2 <orderByField1) {
							return -1;
						}						
					}					
				}
				return o1.compareTo(o2);
			}
		});
		
//		for(Text value : values){
//			String valueStr = value.toString();
//			if(set.size() < topNNumber){
//				set.add(valueStr);
//			}else {
//				if(valueStr.compareTo(set.last()) < 0){
//					set.remove(set.last());
//					set.add(valueStr);					
//				}
//			}
//		}
		
		for(Text value : values){			
			set.add(value.toString());
			if(set.size() > topNNumber){
				set.remove(set.last());
			}
		}
		
		Iterator<String> iter = set.iterator();
		while(iter.hasNext()){
			context.write(key, new Text(iter.next()));
		}
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter","|");
		topNNumber = conf.getInt("top.number", 1);
		String orderModeStr = conf.get("order.mode");/*2:asc,6:asc*/
		
		String[] modeArr= orderModeStr.split(",");
		for(String mode: modeArr){
			String[] split = mode.split(":");
			if(split.length == 2){
				orderMode.put(Integer.parseInt(split[0]), split[1]);
			}
		}

		
	}

}
