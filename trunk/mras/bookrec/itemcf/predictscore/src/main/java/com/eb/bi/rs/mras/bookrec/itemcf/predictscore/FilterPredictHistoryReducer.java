package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterPredictHistoryReducer extends Reducer<TextPair, Text, Text, NullWritable>{
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		// 过滤用户图书历史
		for (Text value:values) {
			String[] fields = value.toString().split("\\|", -1);
			if(fields[0].equals("0")){
				return;
			}
			
			if(fields[0].equals("1")) {
				context.write(new Text(key.getFirst() + "|" + fields[1]), NullWritable.get());
			}
		}
	}
}

