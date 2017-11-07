package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

public class Join4UserBookPrefFinalScoreOnRecRcdMapper extends Mapper<NullWritable, Text, TextPair, TextPair> {
	private TextPair outKey = new TextPair();
	private TextPair outValue = new TextPair();
	
	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {		
		//用户|待推荐图书|源图书集|相似度|属性偏好，eg。u1|a1|1:b1,b3|0.1,0.2,0.3|1.0,4.0,9.0
		String[] fields = value.toString().split("\\|", -1);
		outKey.set(fields[0], "1");
		if(fields.length == 5) {			
			outValue.set(fields[1] + "|" + fields[2] + "|"+ fields[3] + "|"+ fields[4], "1");			
		} else if (fields.length == 4) {
			outValue.set(fields[1] + "|" + fields[2] + "|"+ fields[3], "1");			
		}
		context.write(outKey, outValue);
	}
}
