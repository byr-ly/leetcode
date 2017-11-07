package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinFrequncyMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		//动漫id|频次
		String[] fields = value.toString().split("\\|", -1);
		assert fields.length == 2;
		if( fields.length == 2){
			context.write(new TextPair(fields[0], "1"), new TextPair(fields[1],"1"));
		}	
	}
}
