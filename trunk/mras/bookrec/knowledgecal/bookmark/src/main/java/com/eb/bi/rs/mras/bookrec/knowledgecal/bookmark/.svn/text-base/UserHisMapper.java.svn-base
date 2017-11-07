package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserHisMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//[0]用户id[1]图书id[2]用户阅读章节数[3]总章节数
		String[] field = value.toString().split("\\|");
		
		if(field.length != 4){
			System.out.println("UserHisMapper bad record"+value.toString());
			return;
		}
		
		if(field[2].equals("0")){//没读过也没下载过
			return;
		}
		
/*		String newvalue = "";
		for(int i = 0;i != field.length;i++){
			if(i==1)
				continue;
			
			newvalue = newvalue + field[i] + "|";
		}
*/
		context.write(new Text(field[0]),new Text(field[1]+"|"+field[2]+"|"+field[3]));
	}
}
