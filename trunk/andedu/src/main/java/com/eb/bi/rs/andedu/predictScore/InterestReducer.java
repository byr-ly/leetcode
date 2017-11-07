package com.eb.bi.rs.andedu.predictScore;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


/*
 * 兴趣度计算：e^时间表示兴趣度
 * 输入：key：用户 	value：资讯|兴趣度
 * 输出：key：用户|资讯1|兴趣度1|资讯2|兴趣度2|...
 */
public class InterestReducer extends Reducer<Text, Text, Text, Text> {
	
	Text value = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		String str = new String();
		for(Text value : values) 
		{
			str += "|";
			str += value.toString();
		}
		//value.set(str.replaceFirst(",", ""));
		String key2=key.toString()+str;
		context.write(new Text(key2), value);
		
	}
}
