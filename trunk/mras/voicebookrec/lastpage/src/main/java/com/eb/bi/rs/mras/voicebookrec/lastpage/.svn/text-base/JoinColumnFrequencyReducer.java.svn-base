package com.eb.bi.rs.mras.voicebookrec.lastpage;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinColumnFrequencyReducer extends Reducer<TextPair, Text, Text, Text>{
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		
		Iterator<Text> iter = values.iterator();
		Text columnId = new Text(iter.next());
		while(iter.hasNext()) {
			Text frequency = iter.next();	
			Text outValue = new Text(columnId.toString() + "|" + frequency.toString());
			context.write(key.getFirst(), outValue);			
		}
	}
}
