package com.eb.bi.rs.andedu.predictScore;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 将用户评分过的品牌按两两组合
 */
public class SimilarityMapper extends Mapper<Object, Text, Text, NullWritable> {

	/**
	 * @param 输入格式： 品牌1 |品牌2|评分1|品牌3|评分2|... 输出格式：品牌1|品牌2|相似分
	 */
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\t");
		if (fields.length==2) {

			String user = fields[0];
			String s1 = "";

			String[] right = fields[1].toString().split("\\|");
			for (int i = 0; i < right.length; i++) {
				String[] split = right[i].split(",", -1);
				String bookid = split[0];
				String score = split[1];
				context.write(new Text(user + "|" + bookid + "|" + score),
						NullWritable.get());
			}
		}
	}

}
