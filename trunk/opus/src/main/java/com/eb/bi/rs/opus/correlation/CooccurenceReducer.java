package com.eb.bi.rs.opus.correlation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CooccurenceReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		HashMap<String, Float> id_times = new HashMap<String, Float>();//存储共现次数
		for (Text text : values) {
			String[] id_time = text.toString().split("\\|");
			if (id_times.containsKey(id_time[0])) {
				id_times.put(id_time[0], id_times.get(id_time[0]) + Float.valueOf(id_time[1]));
			}else {
				id_times.put(id_time[0], Float.valueOf(id_time[1]));				
			}
		}
		Iterator<Entry<String, Float>> iterator = id_times.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, Float> entry = iterator.next();
			context.write(key, new Text(entry.getKey()+"|"+entry.getValue()));
			
			if(entry.getKey().equals(key.toString())){
				continue;
			}
			
			context.write(new Text(entry.getKey()), new Text(key+"|"+entry.getValue()));
		}
		
		id_times.clear();
		id_times = null;
	}

}
