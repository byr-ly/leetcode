package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class ClusteringMapper1 extends Mapper<Object, Text, TextPair, Text>{
	private int keyIndex = 0;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		keyIndex = Integer.valueOf(context.getConfiguration().get("Appconf.key.index"));
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|内容
		String[] fields = value.toString().split("\\|");
		
		context.write(new TextPair(fields[keyIndex],"1"),new Text("1|"+value.toString()));
	}
}
