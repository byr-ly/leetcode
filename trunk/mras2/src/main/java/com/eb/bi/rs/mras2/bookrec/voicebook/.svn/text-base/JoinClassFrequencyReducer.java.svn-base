package com.eb.bi.rs.mras2.bookrec.voicebook;

import com.eb.bi.rs.mras2.bookrec.voicebook.util.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinClassFrequencyReducer extends Reducer<TextPair, TextPair, Text, Text>{
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {
		//如果图书已经下架，那么就会出现，一部分图书订购频次记录找不到匹配的分类，此时没有必要输出。	
		Text bookId = key.getFirst();
		Iterator<TextPair> iter = values.iterator();
		TextPair firstPair = iter.next();
		if(firstPair.getSecond().toString().equals("0")){//存在图书信息
			String classType  = firstPair.getFirst().toString();
			while(iter.hasNext()) {
				Text outValue = new Text(classType + "|" + iter.next().getFirst().toString());
				context.write(bookId, outValue);			
			}
		}	
	}
}
