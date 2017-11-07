package com.eb.bi.rs.andedu.itemcf.predictscore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictScoreReducer extends
		Reducer<Text, Text, Text, FloatWritable> {

	private String if_punish_hot_item = "false";
	private String predict_method = "2";
	private Map<String, Float> itemMeanMap;

	/**
	 * key 格式：用户ID|目的动漫ID value 格式：源动漫ID|用户源动漫评分|源动漫和目动漫相似度
	 */
	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		float similaritySum = 0;
		float scoreSum = 0;
		float result = 0;
		String[] fields;

		// 预测公式1，平均分不参与运算
		if (predict_method.equals("1")) {
			for (Text value : values) {
				fields = value.toString().split("\\|", -1);
				if (fields.length != 3) {
					continue;
				}
				float similarity = Float.parseFloat(fields[2]);
				similaritySum += similarity;
				scoreSum += similarity * (Float.parseFloat(fields[1]));
			}

			// 是否对热门物品做惩罚
			if (if_punish_hot_item.equals("true")) {
				if (similaritySum == (float) 0.0) {
					return;
				}
				result = scoreSum / similaritySum;
			} else {
				result = scoreSum;
			}
			context.write(new Text(key), new FloatWritable(result));
			
		} else if (predict_method.equals("2")) { // 预测公式2，平均分参与运算
			for (Text value : values) {
				fields = value.toString().split("\\|", -1);
				if (fields.length != 3) {
					continue;
				}
				float similarity = Float.parseFloat(fields[2]);
				similaritySum += similarity;
				if (!itemMeanMap.containsKey(fields[0])) {
					continue;
				}
				scoreSum += similarity
						* (Float.parseFloat(fields[1]) - itemMeanMap
								.get(fields[0]));
			}

			fields = key.toString().split("\\|", -1);
			if (similaritySum == (float) 0.0
					|| !itemMeanMap.containsKey(fields[1])) {
				return;
			}
			result = itemMeanMap.get(fields[1]) + scoreSum / similaritySum;
			context.write(new Text(key), new FloatWritable(result));
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		if_punish_hot_item = context.getConfiguration().get(
				"if_punish_hot_item");
		predict_method = context.getConfiguration().get("predict_method");
		URI[] localFiles = context.getCacheFiles();
		itemMeanMap = new HashMap<String, Float>();
		for (URI localFile : localFiles) {
			BufferedReader br = null;
			String line = null;
			try {
				Path path = new Path(localFile.getPath());
				String file = path.getName();
				br = new BufferedReader(new FileReader(file));
				while ((line = br.readLine()) != null) {
					String[] fields = line.split("\\|");
					if (fields.length != 2) {
						continue;
					}
					itemMeanMap.put(fields[0], Float.parseFloat(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}

}
