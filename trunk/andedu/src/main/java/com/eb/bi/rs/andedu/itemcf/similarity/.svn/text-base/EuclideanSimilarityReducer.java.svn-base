package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 欧式距离方法计算物品两两之间相似度
 */
public class EuclideanSimilarityReducer extends
		Reducer<Text, DoubleWritable, Text, Text> {

	// 是否对活跃用户做惩罚,true or false
	String if_punish_active_users = "false";
	// 是否对相似度归一化
	String if_norm = "false";
	double maxSim = 0;

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		double sumIJdiff2 = 0;
		double diff = 0;
		for (DoubleWritable value : values) {
			count++;
			diff = value.get();
			sumIJdiff2 += diff * diff;
		}
		double sim = 1.0 / (1.0 + Math.sqrt(sumIJdiff2));
		if (if_punish_active_users.equals("true")) {
			sim = sim / (1 + Math.log(1 + count));
		}
		
		context.write(key, new Text(String.format("%f|%d", sim, count)));
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		if_punish_active_users = context.getConfiguration().get(
				"if_punish_active_users");
		if_norm = context.getConfiguration().get("if_norm");
	}

}
