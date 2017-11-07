package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinScoreMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	/*用户ID|图书ID|打分*/
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		if( fields.length == 3){
			context.write(new TextPair(fields[1], "1"), new Text("1|" + fields[0] + "|" + fields[2]));
		}	
	}
}
