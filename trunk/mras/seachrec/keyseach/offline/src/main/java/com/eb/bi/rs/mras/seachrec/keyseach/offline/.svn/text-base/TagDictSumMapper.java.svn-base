package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagDictSumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	HashMap<String, Integer> sumMap = new HashMap<String,Integer>();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String[] fields = value.toString().split("\\|");		
		String keyWord = fields[0];	
		int weight = Integer.parseInt(fields[2]);
		if(sumMap.containsKey(keyWord)){
			sumMap.put(keyWord, sumMap.get(keyWord) + weight);
		}
		else {
			sumMap.put(keyWord, weight);
		}
			
	}


	protected void cleanup(Context context) throws IOException ,InterruptedException {		
		Iterator<Entry<String, Integer>> iter = sumMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, Integer> entry = iter.next();
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			
		}
	}	
}

