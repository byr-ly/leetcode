package com.eb.bi.rs.anhui.moduledev.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 用户评分信息按一个一个的品牌拆分（方便计算余弦相似度的分母）
 */
public class BrandScoreMapper extends Mapper<Object, Text, Text, Text> {


	/**
	 * value 格式：用户|品牌1|评分1|品牌2|评分2|...
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\\|");
		
		if(fields.length<3){
			return;
		}
		for(int i=1;i<fields.length-1;i=i+2){
			String brandId = fields[i];
			String score = fields[i+1];
			System.out.println(brandId+"|"+score);
			context.write(new Text(brandId), new Text(score));
		}

		
	}
	
}
