package com.eb.bi.rs.anhui.moduledev.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 用户评分信息（用来过滤用户评过分的品牌）
 */
public class UserBrandMapper extends Mapper<Object, Text, Text, Text> {


	/**
	 * @param value 格式：用户|品牌1|评分1|品牌2|评分2|...
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		
		if (fields.length < 3) {
			return;
		}
		String userId = fields[0];
		context.write(new Text(userId), new Text("0|"+value.toString()));
	}
	
}
