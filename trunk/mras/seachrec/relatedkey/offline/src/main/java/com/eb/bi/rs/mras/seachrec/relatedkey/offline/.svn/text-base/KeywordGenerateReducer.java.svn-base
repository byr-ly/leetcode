package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KeywordGenerateReducer extends Reducer<Text,Text, Text, Text> {
	//private Set<String> usefulWord = new HashSet<String>();
	/*
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		
		//载入图书名称列表,作者名称列表
		
	}
	*/
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Set<String> userbookList = new HashSet<String>();
		for(Text value:values){
			String sentence = value.toString();	
			userbookList.add(sentence);
		}
		
		for(String bookname:userbookList){
			//if(usefulWord.contains(bookname))
			context.write(key,new Text(bookname));
		}
	}
}
