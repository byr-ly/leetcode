package com.eb.bi.rs.anhui.moduledev.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 将用户评分过的品牌按两两组合
 */
public class SimilarityMapper extends Mapper<Object, Text, Text, Text> {


	/**
	 * @param value 格式：用户|品牌1|评分1|品牌2|评分2|...
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");

		if (fields.length < 3 ) return;
        for(int i=1;i<fields.length-1;i=i+2){
        	for(int j=i+2;j<fields.length-1;j=j+2){
        		context.write(new Text(fields[i]+"|"+fields[j]), new Text(fields[i+1]+"|"+fields[j+1]));
        	}
        }		
	}
	
}
