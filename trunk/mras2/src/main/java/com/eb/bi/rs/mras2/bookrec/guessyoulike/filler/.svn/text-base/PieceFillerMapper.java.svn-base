package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PieceFillerMapper extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//图书|事业部id
		String[] fields = value.toString().split("\\|");
		//事业部id|图书
		context.write(new Text(fields[1]),new Text(fields[0]));
	}
}
