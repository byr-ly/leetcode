package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

public class UserRecBookPrefViaPropertyOnRecRcdSideMapper extends Mapper<NullWritable, Text, TextPair, TextPair>{
	private TextPair outKey = new TextPair();
	private TextPair outValue = new TextPair();
	@Override
	protected void map(NullWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException {
		//用户|待推图书|源图书集|预测偏好向量
		String[] fields = value.toString().split("\\|", -1);
		outKey.set(fields[0], "1");
		if(fields.length == 4) {			
			outValue.set(fields[1] + "|" + fields[2] + "|" + fields[3], "1");
			context.write(outKey, outValue);
		}		
	}
}
