package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算各个物品的TopN相似物品
 */
public class TopSimilarItemReducer extends
		Reducer<Text, Text, Text, FloatWritable> {

	private int topN;

	/**
	 * key:源物品 value 格式：目的物品|相似度
	 */
	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Map<String, Float> simMap = new HashMap<String, Float>();
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			simMap.put(fields[0], Float.parseFloat(fields[1]));
		}

		List<Map.Entry<String, Float>> scoreEntries = new ArrayList<Map.Entry<String, Float>>(
				simMap.entrySet());
		Collections.sort(scoreEntries,
				new Comparator<Map.Entry<String, Float>>() {

					@Override
					public int compare(Entry<String, Float> o1,
							Entry<String, Float> o2) {
						return o2.getValue().compareTo(o1.getValue());
					}
				});

		int count = 0;
		for (int i = 0; i < scoreEntries.size() && count < topN; i++) {
			context.write(new Text(key.toString() + "|" + scoreEntries.get(i).getKey()), new FloatWritable(
					scoreEntries.get(i).getValue()));
			count++;
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		topN = Integer.parseInt(conf.get("conf.top.similarity.num"));
	}
}
