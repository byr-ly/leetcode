package com.eb.bi.rs.mras2.voicebookrec.lastpage;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinBookCoocurrenceReducer extends Reducer<TextPair, Text, Text, NullWritable>{
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		
		Iterator<Text> iter = values.iterator();
		Text bookId = new Text(iter.next());
		if(!iter.hasNext()){
			context.write(bookId, NullWritable.get());
		}		
		while(iter.hasNext()) {
			Text coocurrenceInfo = iter.next();	
			Text outkey = new Text(bookId.toString() + "|" + coocurrenceInfo.toString());
			context.write(outkey, NullWritable.get());
		}

	}
}
