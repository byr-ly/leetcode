package com.eb.bi.rs.qhll.userapprec.predictpref;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class ItemConvertReducer extends Reducer<Text, Text, IntWritable, Text> {
	
	private final static HashMap<String, String> itemMap = new HashMap<String, String>();
	private int idx = 1;
	private IntWritable result = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		for (Text value:values) 
		{
			itemMap.put(value.toString(), new String(""));
		}
		System.out.println("size of result map is " + itemMap.size());
		
		Iterator iter = itemMap.entrySet().iterator();
		while(iter.hasNext())
		{
			Map.Entry entry = (Map.Entry) iter.next();
			result.set(idx);
			context.write(result, new Text(entry.getKey().toString()));
			idx++;
		}
	}
}
