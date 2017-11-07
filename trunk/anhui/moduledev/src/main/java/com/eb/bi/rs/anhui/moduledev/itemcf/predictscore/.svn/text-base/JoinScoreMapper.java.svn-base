package com.eb.bi.rs.anhui.moduledev.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	/**
	 * @param value 格式：用户|品牌1|评分1|品牌2|评分2|...
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
        
		String[] fields = value.toString().split("\\|");
		if (fields.length < 3) {
			return;
		}
		String userId = fields[0];
		String brandId = null;
		String score = null;
		for(int i=1;i<fields.length-1;i+=2){
			brandId = fields[i];
			score = fields[i+1];
			context.write(new Text(brandId), new Text("0|"+userId+"|"+score));
		}	
	}
}
