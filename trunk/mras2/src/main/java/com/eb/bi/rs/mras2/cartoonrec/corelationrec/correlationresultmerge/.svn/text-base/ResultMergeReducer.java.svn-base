package com.eb.bi.rs.mras2.cartoonrec.corelationrec.correlationresultmerge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultMergeReducer extends Reducer<Text, Text, Text, Text> {
	//
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//对于传进来的数据book_id|similarity|book_id|similarity，要把boo_id和similarity数据放到HashMap中，对于重读的book_id，取similarity的最大值
		Map<String, String> results = new HashMap<String, String>();
		for(Text value : values) {
			String[] items = value.toString().split("\\|", -1);
			assert items.length == 2;
			if(!results.containsKey(items[0])) {
				results.put(items[0], Double.parseDouble(items[1]) > 0.6 ? "0.6" : items[1]);
			} else {
				if(Double.parseDouble(items[1]) > 0.6) {
					results.put(items[0], "0.6");
				} else {
					results.put(items[0], items[1].compareTo(results.get(items[0])) >= 0 ? items[1] : results.get(items[0]));
				}
			}
		}
		System.out.println(results);
		//遍历HashMap，把结果拼接成一个String
		StringBuffer sb = new StringBuffer();
		for(Entry<String, String> entry : results.entrySet()) {
			sb.append(entry.getKey() + "," + entry.getValue() + "|");
		}
		if(sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		context.write(key, new Text(sb.toString()));
	}
}
