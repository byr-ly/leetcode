package com.eb.bi.rs.mras.bookrec.voicebook;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListenCorelationBookRecMapper extends Mapper<LongWritable, Text, Text, Text>{

	private int listenCorelationRecNum;
	private HashMap<String, TreeSet<String>>  listenIndicatorMap = new HashMap<String, TreeSet<String>>();
	
	
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype		
		String[] fields = value.toString().split("\\|",-1);
		assert fields.length == 10;		
		String bookId = fields[0];
		
		if(listenIndicatorMap.containsKey(bookId)){			
			TreeSet<String> set = listenIndicatorMap.get(bookId);
			set.add(value.toString());
			if(set.size() > listenCorelationRecNum) {
				set.remove(set.last());
			}
			
		}else {
			TreeSet<String> set = new TreeSet<String>(new Comparator<String>() {
				public int compare(String o1, String o2) {					
					String[] fields1 = o1.split("\\|",-1);
					String[] fields2 = o2.split("\\|",-1);
					int ret = fields1[9].compareTo(fields2[9]);
					if(ret != 0) return ret;					
					double Confidence1 = Double.parseDouble(fields1[5]);
					double confidence2 = Double.parseDouble(fields2[5]);					
					if(confidence2 > Confidence1){
						return 1;
					}else if(confidence2 < Confidence1){
						return -1;
					}else {
						return fields1[1].compareTo(fields2[1]);
					}			
				}
			});
			
			set.add(value.toString());
			listenIndicatorMap.put(bookId, set);
		}	
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {		
		listenCorelationRecNum = context.getConfiguration().getInt("listen.corelation.recommend.number", 20);		
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		Iterator<Entry<String, TreeSet<String>>> iterator = listenIndicatorMap.entrySet().iterator();    	
    	while(iterator.hasNext()){
    		Entry<String, TreeSet<String>> item = iterator.next();
    		String bookId = item.getKey();
    		TreeSet<String> set = item.getValue();
    		for (String record : set) {
    			context.write(new Text(bookId), new Text(record));				
			}
    	}
	}
}




