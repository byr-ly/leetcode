package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author ynn 考虑用户评分平均值的影响,生成新的用户物品评分矩阵
 */
public class GenerateNewUserItemScoreReducer extends
		Reducer<TextPair, Text, Text, NullWritable> {

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		float mean = 0;
		float score = 0;
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			// 0|0|平均分
			if (fields[0].equals("0")) {
				mean = Float.parseFloat(fields[2]);
				continue;
			}

			// 1|物品|评分
			if (fields[0].equals("1")) {
				score = Float.parseFloat(fields[2]);
				score = score - mean;
				context.write(new Text(key.getFirst() + "|" + fields[1] + "|"
						+ score), NullWritable.get());
			}
		}
	}
}
