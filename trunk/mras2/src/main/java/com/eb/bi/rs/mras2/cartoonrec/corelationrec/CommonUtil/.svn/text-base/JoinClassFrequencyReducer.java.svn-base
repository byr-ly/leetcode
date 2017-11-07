package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinClassFrequencyReducer extends Reducer<TextPair, TextPair, Text, Text>{
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {
		//如果图书已经下架，那么就会出现，一部分图书订购频次记录找不到匹配的分类，此时没有必要输出。	
		//也就是说进行的内连接。
		Text bookId = key.getFirst();
		Iterator<TextPair> iter = values.iterator();
		TextPair firstPair = iter.next();
		if(firstPair.getSecond().toString().equals("0")){//存在图书信息      
			String bookinfo  = firstPair.getFirst().toString();
			if(iter.hasNext()) {
				//book_id动漫ID|author_id作者ID|class_id分类ID|charge_type计费类型|点击量|频次
				Text outValue = new Text(bookinfo + "|" + iter.next().getFirst().toString());
				context.write(bookId, outValue);			
			}else{
				Text outValue = new Text(bookinfo + "|0");
				context.write(bookId, outValue);
			}
		}	
	}
}
