package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.StringDoublePair;

public class SelectFinalRecResultMapper extends Mapper<Object, Text, Text, StringDoublePair>{
	//用户|图书|源图书源|得分	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 4) {
			context.write(new Text(fields[0]), new StringDoublePair(fields[1] + "|" + fields[2],Double.parseDouble(fields[3])));			
		}
	}
}
