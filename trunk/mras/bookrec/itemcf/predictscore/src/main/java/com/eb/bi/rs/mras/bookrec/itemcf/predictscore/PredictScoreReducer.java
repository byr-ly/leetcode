package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

/**
 * 预测图书得分
 */
public class PredictScoreReducer extends Reducer<Text, Text, Text, FloatWritable> {

	private Map<String, Float> bookMeanMap;

	/**
	 * @param value 格式：用户源图书评分|源图书ID|源图书和目的图书相似度
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		float simlaritySum = (float) 0.0;
		float scoreSum = (float) 0.0;
		
		String[] fields;
		for (Text value : values) {
			fields = value.toString().split("\\|", -1);
			if (fields.length != 3) {
				continue;
			}
			float simlarity = Float.parseFloat(fields[2]);
			simlaritySum += simlarity; 
			if (!bookMeanMap.containsKey(fields[1])) {
				continue;
			}
			scoreSum += simlarity*(Float.parseFloat(fields[0]) - bookMeanMap.get(fields[1]));
		}
		
		fields = key.toString().split("\\|", -1);
		if (simlaritySum == (float) 0.0 || !bookMeanMap.containsKey(fields[1])) {
			return;
		}
		float result = bookMeanMap.get(fields[1]) + scoreSum/simlaritySum;
		context.write(new Text(key), new FloatWritable(result));
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		bookMeanMap = new HashMap<String, Float>();
		for (Path path : localFiles) {
			BufferedReader br = null;
			try {
				String file = path.toString();
				br = new BufferedReader(new FileReader(file));
				String line;
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
