package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn 余弦相似度方法计算单个用户对两个物品之间相似度的贡献信息
 */
public class CosineSimilarityMapper extends
		Mapper<Object, Text, Text, CosineSimilarityWritable> {

	/**
	 * value 格式： 物品I|物品J|评分I|评分J
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

		context.write(new Text(i + "|" + j), new CosineSimilarityWritable(productij,
				squarei, squarej));

	}
}
