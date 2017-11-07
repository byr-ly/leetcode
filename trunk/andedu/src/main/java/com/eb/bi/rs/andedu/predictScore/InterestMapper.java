package com.eb.bi.rs.andedu.predictScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InterestMapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * 输入格式：用户A	新闻1：时间1，新闻2：时间2...
	 * 输出格式：用户A	新闻1：兴趣度1，新闻2，兴趣度2...
	 */
//	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//		String[] fields = value.toString().split("\t");
//		if(fields.length!=2){
//			return;
//		}
//		String[] str = fields[1].split(",");
//		
//		for(int i=0;i<str.length;i++){
//			String str1 = str[i].split(":")[0];
//			String str2 = str[i].split(":")[1];
//
//			double d = Math.pow(Math.E, Double.parseDouble(str2));
//			String b = String.valueOf(d);
//			String result = str1 + ":" + b;
//			context.write(new Text(fields[0]),new Text(result)); //key:用户    value:新闻1：兴趣度1
//		}
//		
//		
//		
//	}
	
	String dateNow=null;
	int yearNow,monthNow,dayNow,yearLast,monthLast,dayLast,dayChange;
	@Override
	/*
	 * @param 
	 * 根据用户行为时间通过e^时间计算兴趣度，
	 * 输入格式：
	 * 		      用户id|资讯id1，最后一次点击时间（yyyyMMdd）|资讯id2，最后一次点击时间|…
	 * 输出格式：key:用户       value:资讯|兴趣度
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
        
		
		String[] fields = value.toString().split("\\|");
		//String[] mid=null;
		if (fields.length < 3) {
			return;
		}
		String userId = fields[0];
		String brandId = null;
		String score = null;
		for(int i=1;i<fields.length-1;i++){
			
			String[] mid=fields[i].toString().split(",");
			
			brandId = mid[0];
			score = mid[1];
			yearNow=Integer.parseInt(dateNow.substring(0, 4));
			monthNow=Integer.parseInt(dateNow.substring(4, 6));
			dayNow=Integer.parseInt(dateNow.substring(6, 8));
			yearLast=Integer.parseInt(mid[1].substring(0, 4));
			monthLast=Integer.parseInt(mid[1].substring(4, 6));
			dayLast=Integer.parseInt(mid[1].substring(6, 8));
			
			dayChange=(yearNow-yearLast)*365+(monthNow-monthLast)*30+(dayNow-dayLast);
			if(dayChange>=130){
				dayChange=130;
			}
			
			double d = Math.pow(Math.E, -dayChange);
			score = String.valueOf(d);
			context.write(new Text(userId), new Text(brandId+"|"+score));
		}	
	}
	
	public void setup(Context context) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		dateNow=conf.get("datenow");
	}
}
