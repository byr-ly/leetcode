package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSimilarityMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		if( fields.length == 3){
			context.write(new TextPair(fields[0], "0"), new Text("0|" + fields[1] + "|" + fields[2]));
		}	
	}
}
