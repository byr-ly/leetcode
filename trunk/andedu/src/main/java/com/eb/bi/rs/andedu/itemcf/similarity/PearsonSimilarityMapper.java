package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn Pearson方法计算单个用户对两个物品之间相似度的贡献信息
 */
public class PearsonSimilarityMapper extends
		Mapper<Object, Text, Text, PearsonSimilarityWritable> {

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

		context.write(new Text(i + "|" + j), new PearsonSimilarityWritable(scorei, scorej));

	}
}
