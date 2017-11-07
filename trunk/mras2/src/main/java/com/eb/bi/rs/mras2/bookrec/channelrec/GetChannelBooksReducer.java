package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetChannelBooksReducer extends Reducer<Text, Text, Text, NullWritable> {
	private int channelRecNum;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String , Double> bookScore = new HashMap<String , Double>();
		for (Text text : values) {
			String[] fields = text.toString().split("\\|");
			if (fields.length == 2) {
				String book = fields[0];
				Double score = Double.parseDouble(fields[1]);
				bookScore.put(book, score);
			}
		}
		String channelRecBooks = sort(bookScore);
		
		context.write(new Text(key.toString() + ";" + channelRecBooks), NullWritable.get());
	}
	
	private String sort(Map<String , Double> bookScore) {
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>( bookScore.entrySet() );  
        Collections.sort( list, new Comparator<Map.Entry<String, Double>>()  
        {  
            public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )  
            {  
                return (o2.getValue()).compareTo( o1.getValue() );  
            }  
        } );  
  
        StringBuffer channelRecBooks = new StringBuffer();
        int n = list.size() < channelRecNum ? list.size():channelRecNum;
        for (int i = 0; i < n; i++) {
        	channelRecBooks.append(list.get(i).getKey()+","+list.get(i).getValue()+"|");
		} 
		return channelRecBooks.toString();
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		channelRecNum = Integer.parseInt(conf.get("channelRecNum"));
	}
	
	

}
