package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Pearson方法计算物品两两之间相似度
 */
public class PearsonSimilarityReducer extends
		Reducer<Text, PearsonSimilarityWritable, Text, Text> {

	Map<String, Float> objAvgMap = new HashMap<String, Float>();

	// 是否对活跃用户做惩罚,true or false
	String if_punish_active_users = "false";
	// 是否对相似度归一化
	String if_norm = "false";

	protected void reduce(Text key, Iterable<PearsonSimilarityWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		double sumI2 = 0;
		double sumJ2 = 0;

		double numerator = 0;
		double denominator = 0;

		double scoreI = 0;
		double scoreJ = 0;

		String[] fields = key.toString().split("\\|");
		String I = fields[0];
		String J = fields[1];
		double avgI = objAvgMap.get(I);
		double avgJ = objAvgMap.get(J);

		for (PearsonSimilarityWritable value : values) {
			count++;
			scoreI = value.getI();
			scoreJ = value.getJ();
			numerator += (scoreI - avgI) * (scoreJ - avgJ);
			sumI2 += (scoreI - avgI) * (scoreI - avgI);
			sumJ2 += (scoreJ - avgJ) * (scoreJ - avgJ);
		}

		denominator = Math.sqrt(sumI2) * Math.sqrt(sumJ2);

		if (denominator != 0) {
			double sim = numerator / denominator;
			if (if_punish_active_users.equals("true")) {
				sim = sim / (1 + Math.log(1 + count));
			}
			context.write(key, new Text(String.format("%f|%d", sim, count)));
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		if_punish_active_users = config.get("if_punish_active_users");
		if_norm = config.get("if_norm");
		URI[] localFiles = context.getCacheFiles();
		for (URI localFile : localFiles) {
			String line = null;
			BufferedReader br = null;
			try {
				Path path = new Path(localFile.getPath());
				br = new BufferedReader(new FileReader(path.getName()));
				while ((line = br.readLine()) != null) {
					String fields[] = line.split("\\|");
					if (fields.length < 2) {
						continue;
					}
					objAvgMap.put(fields[0], Float.parseFloat(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}

}
