package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BookTypeReducer extends Reducer<Text,Text, Text, Text> {
	private static String book_Ctype1;
	private static String book_Ctype2;
	private static String book_Ctype3;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		book_Ctype1 = context.getConfiguration().get("charge_type1");
		book_Ctype2 = context.getConfiguration().get("charge_type2");
		book_Ctype3 = context.getConfiguration().get("charge_type3");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//图书总章节数
		int book_chnum = 0;
		//图书订购类型
		String book_ctype = "";
		//图书打分类型
		int book_TYPE = 0;
		
		for(Text value:values){
			String[] field = value.toString().split("\\|");
			
			if(field[0].equals("0")){
				book_chnum=Integer.valueOf(field[1]);
			}
			else{
				book_ctype=field[1];
			}
		}
		
		if(book_ctype.equals(book_Ctype1)){
			if(book_chnum<=50&&book_chnum>0){
				book_TYPE = 1;
			}
			if(book_chnum>50&&book_chnum<=250){
				book_TYPE = 2;
			}
			if(book_chnum>250&&book_chnum<=600){
				book_TYPE = 3;
			}
			if(book_chnum>600){
				book_TYPE = 4;
			}
		}
		if(book_ctype.equals(book_Ctype2)){
			if(book_chnum<=50&&book_chnum>0){
				book_TYPE = 5;
			}
			if(book_chnum>50&&book_chnum<=200){
				book_TYPE = 6;
			}
			if(book_chnum>200){
				book_TYPE = 7;
			}
		}	
		if(book_ctype.equals(book_Ctype3)){
			if(book_chnum<=50&&book_chnum>0){
				book_TYPE = 8;
			}
			if(book_chnum>50){
				book_TYPE = 9;
			}
		}
		
		//context.write(key,new Text(book_TYPE+"|"+book_chnum));
		context.write(key,new Text(book_TYPE + ""));
	}
}
