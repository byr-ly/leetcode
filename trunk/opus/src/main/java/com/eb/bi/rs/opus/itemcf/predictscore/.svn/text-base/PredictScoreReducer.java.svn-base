package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;

/**
 * 预测动漫得分
 */
public class PredictScoreReducer extends
		Reducer<Text, Text, Text, FloatWritable> {
	private Map<String, Float> bookMeanMap;

	/**
	 * @param key
	 *            格式：用户ID|目的动漫ID value 格式：源动漫ID|用户源动漫评分|源动漫和目动漫相似度
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float similaritySum = (float) 0.0;
		float scoreSum = (float) 0.0;
		System.out.println("start...");;
		String[] fields;
		for (Text value : values) {
			fields = value.toString().split("\\|", -1);
			System.out.println("Values: " + value.toString());
			if (fields.length != 3) {
				continue;
			}
			float similarity = Float.parseFloat(fields[2]);
			similaritySum += similarity;
			if (!bookMeanMap.containsKey(fields[0])) {
				continue;
			}
			scoreSum += similarity * (Float.parseFloat(fields[1]) - bookMeanMap.get(fields[0]));
		}

		fields = key.toString().split("\\|", -1);
		if (similaritySum == (float) 0.0 || !bookMeanMap.containsKey(fields[1])) {
			return;
		}
		float result = bookMeanMap.get(fields[1]) + scoreSum / similaritySum;
		context.write(new Text(key), new FloatWritable(result));
	}

	/**
	 * 加载平均分，格式：动漫|平均分
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] localFiles = context.getCacheFiles();
		bookMeanMap = new HashMap<String, Float>();
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
					bookMeanMap.put(fields[0], Float.parseFloat(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}
}
