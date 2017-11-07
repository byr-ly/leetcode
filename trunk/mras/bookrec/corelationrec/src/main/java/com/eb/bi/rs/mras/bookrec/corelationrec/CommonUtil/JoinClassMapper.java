package com.eb.bi.rs.mras.bookrec.corelationrec.CommonUtil;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinClassMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		/*按照图书ID|作者|分类|大类*/
		String[] fields = value.toString().split("\\|", -1);
		if(fields.length >= 4){
			context.write(new TextPair(fields[0], "0"), new TextPair(fields[1]+"|"+fields[2]+"|"+fields[3],"0"));							
		}
	}
	

}
