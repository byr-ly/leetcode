package com.eb.bi.rs.andedu.predictScore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 预测过的品牌打分
 */
public class BrandPredictScoreMapper extends Mapper<Object, Text, Text, Text> {


	/**
	 * @param value 格式：用户|目的品牌|目的品牌的预测打分
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length < 3 ) return;
		
        context.write(new Text(fields[0]), new Text("1|"+value.toString()));
        		
	}
	
}
