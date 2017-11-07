package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

public class UserRecBookPrefViaPropertyOnUserPropertyPrefSideMapper extends Mapper<LongWritable, Text, TextPair, TextPair>{

	private TextPair outKey = new TextPair();;
	private TextPair outvalue = new TextPair();
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException {
		//用户|属性1|属性2|。。。。|属性n
		String valueStr = value.toString();	
		outKey.set(valueStr.substring(0, valueStr.indexOf("|")), "0");
		outvalue.set(valueStr, "0");
		context.write(outKey, outvalue);	
	}

}
