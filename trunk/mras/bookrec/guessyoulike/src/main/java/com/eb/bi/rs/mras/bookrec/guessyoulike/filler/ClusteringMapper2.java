package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class ClusteringMapper2 extends Mapper<Object, Text, TextPair, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|群id
		String[] fields = value.toString().split("\\|");
		
		context.write(new TextPair(fields[0],"0"),new Text("0|"+value.toString()));
	}
}
