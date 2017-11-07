package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ynn 2016-07-21 计算单个用户对两个动漫之间相似度的贡献信息
 */
public class SimilariryMapper extends
		Mapper<Object, Text, Text, SimilarityWritable> {

	/**
	 * value 格式：动漫I|动漫J|评分I|评分J
	 */
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 4) {
			return;
		}

		String i = fields[0];
		String j = fields[1];

		double scorei = Double.parseDouble(fields[2]);
		double scorej = Double.parseDouble(fields[3]);

		double squarei = scorei * scorei;
		double squarej = scorej * scorej;

		double productij = scorei * scorej;

		context.write(new Text(i + "|" + j), new SimilarityWritable(productij,
				squarei, squarej));
	}

}
