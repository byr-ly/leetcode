package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;


public class Join4UserBookPrefFinalScoreOnUserMaxPrefsMapper extends Mapper< NullWritable, Text, TextPair, TextPair> {
	private TextPair outKey = new TextPair();
	private TextPair outValue = new TextPair();
	
	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {
		//u1|0.4,0.9,0.3|7.0,16.0,27.0
		String[] fields = value.toString().split("\\|", -1);
		outKey.set(fields[0], "0");
		if(fields.length == 3) {			
			outValue.set(fields[1] + "|" + fields[2] , "0");			
		} else if (fields.length == 2) {
			outValue.set(fields[1], "0");			
		}
		context.write(outKey, outValue);
	}
}
