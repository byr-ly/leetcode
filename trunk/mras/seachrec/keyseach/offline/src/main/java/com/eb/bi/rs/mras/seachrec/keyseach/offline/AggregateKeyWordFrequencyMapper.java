package com.eb.bi.rs.mras.seachrec.keyseach.offline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AggregateKeyWordFrequencyMapper extends Mapper<Text, Text, Text, IntWritable> {

	private HashMap<String,Integer>	keyWords = new HashMap<String,Integer>();

	@Override	
	protected void map(Text key, Text value, Context context) 
		throws IOException, InterruptedException {
		
		String keyWord = key.toString();
		int count = Integer.parseInt(value.toString());
		if(keyWords.containsKey(keyWord)){
			keyWords.put(keyWord, keyWords.get(keyWord) + count);
		}else {
			keyWords.put(keyWord, count);
		}	

	 }
	
	protected void cleanup(Context context) throws IOException ,InterruptedException
	{
		Iterator<Entry<String, Integer>> iter = keyWords.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, Integer> entry = iter.next();
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
	}
}

