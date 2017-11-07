package com.eb.bi.rs.qhll.userapprec.predictpref;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ItemConvertMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	private final static HashMap<String, String> itemMap = new HashMap<String, String>();
	
	@Override	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {					
		String fields[] = value.toString().split("\\|");
		if(fields.length != 3 ){
			return;
		}
		itemMap.put(fields[1], "");
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException,	InterruptedException {
		Iterator iter = itemMap.entrySet().iterator();
		System.out.println("size of map is " + itemMap.size());
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry) iter.next();
			context.write(new Text("K"), new Text(entry.getKey().toString()));
		}
		super.cleanup(context);		
	}
}
