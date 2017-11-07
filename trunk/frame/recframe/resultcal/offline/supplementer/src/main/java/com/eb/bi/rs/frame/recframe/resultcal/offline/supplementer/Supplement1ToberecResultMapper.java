package com.eb.bi.rs.frame.recframe.resultcal.offline.supplementer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Supplement1ToberecResultMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key,new Text("0|"+value.toString()));
	}
}
