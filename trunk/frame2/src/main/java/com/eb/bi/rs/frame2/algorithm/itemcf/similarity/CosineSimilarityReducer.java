package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

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
 * 余弦相似度方法计算物品两两之间相似度
 */
public class CosineSimilarityReducer extends
		Reducer<Text, CosineSimilarityWritable, Text, Text> {

	Map<String, Float> objModuleMap = new HashMap<String, Float>();
	// 1:分母只考虑对两个物品同时有评分的用户,2:分母利用对两个无匹分别有评分的用户
	String method = "2";
	// 是否对活跃用户做惩罚,true or false
	String if_punish_active_users = "false";

	protected void reduce(Text key, Iterable<CosineSimilarityWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		double sumProduct = 0;
		double moduleI = 0;
		double moduleJ = 0;
		double sumI = 0;
		double sumJ = 0;
		String[] fields = key.toString().split("\\|");
		String I = fields[0];
		String J = fields[1];

		for (CosineSimilarityWritable value : values) {
			count++;
			sumProduct += value.getProductij();
			sumI += value.getSquarei();
			sumJ += value.getSquarej();
		}
		double module = 0;
		// 分母只考虑对两个物品同时有评分的用户
		if ("1".equals(method)) {
			module = Math.sqrt(sumI) * Math.sqrt(sumJ);
		} else if ("2".equals(method)) {// 分母利用对两个物品分别有评分的用户
			moduleI = objModuleMap.get(I);
			moduleJ = objModuleMap.get(J);
			module = moduleI * moduleJ;
		}

		if (sumProduct > 0 && module > 0) {
			double sim = sumProduct / module;
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
		method = config.get("cos_method");
		if_punish_active_users = config.get("if_punish_active_users");
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
					objModuleMap.put(fields[0], Float.parseFloat(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}
}
