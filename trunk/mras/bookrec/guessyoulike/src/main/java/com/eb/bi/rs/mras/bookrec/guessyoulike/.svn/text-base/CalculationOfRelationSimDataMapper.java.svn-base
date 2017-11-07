package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class CalculationOfRelationSimDataMapper extends Mapper<NullWritable, Text, TextPair, Text>{
	private int partNum;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		partNum = Integer.valueOf(context.getConfiguration().get("Appconf.user.part.num"));
	}
	
	@Override
	protected void map(NullWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		//源图书|目标图书|相似度向量
		String[] fields = value.toString().split("\\|");
		
		for(int i = 0; i!= partNum; i++){
			//key:源图书|partNum;tag:0;val:0|目标图书|相似度向量
			context.write(new TextPair(fields[0]+"|"+i,"0"),new Text("0|"+fields[1]+"|"+fields[2]));
		}
	}
}
