package com.eb.bi.rs.anhui.moduledev.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 按 用户|目的品牌拆分
 */
public class PredictScoreMapper extends Mapper<Object, Text, Text, Text> {


	/**
	 * @param value 格式：用户|目的品牌|相似分
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		
		if (fields.length < 3) {
			return;
		}
		String userId = fields[0];
		String brandId = fields[1];
		String score = fields[2];
		context.write(new Text(userId+"|"+brandId), new Text(score));

	}
	
}
