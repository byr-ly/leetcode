package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

public class CalculationOfRelationUserDataMapper extends Mapper<NullWritable, Text, TextPair, Text>{
	private int partNum;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		partNum = Integer.valueOf(context.getConfiguration().get("Appconf.user.part.num"));
	}
	
	@Override
	protected void map(NullWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		//用户|源图书|图书打分|来源集
		String[] fields = value.toString().split("\\|");
		
		int uNum = Integer.valueOf(fields[0].substring(fields[0].length()-4, fields[0].length()-2));
		
		//key:源图书|partNum;tag:1;val:1|用户|图书打分|来源集
		context.write(new TextPair(fields[1]+"|"+uNum%partNum,"1"),new Text("1|"+fields[0]+"|"+fields[2]+"|"+fields[3]));
	}
}
