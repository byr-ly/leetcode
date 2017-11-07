package com.eb.bi.rs.anhui.moduledev.itemcf.similarity;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 将各个品牌的评分在余弦相似度公式分母的部分计算出来
 * cos = 两个品牌都打过分的用户/（用过第一个品牌的全部用户  用过第二个品牌的全部用户）【计算的不是用户数，此处只是列出分子分母数据来源，方便理解】
 */
public class BrandScoreReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Double scores = 0.0;
		for (Text value : values) {
			scores +=  Math.pow(Double.parseDouble(value.toString()), 2);
		}

		String result = key.toString()+"|"+Math.sqrt(scores);
        context.write(new Text(result),new Text());
		
	}
}

