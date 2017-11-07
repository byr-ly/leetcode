package com.eb.bi.rs.mras2.bookrec.voicebook;

import com.eb.bi.rs.mras2.bookrec.voicebook.util.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class JoinFrequncyMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String[] fields = value.toString().split("\\|", -1);
		assert fields.length == 2;
		if( fields.length == 2){
			context.write(new TextPair(fields[0], "1"), new TextPair(fields[1],"1"));
		}	
	}
}
