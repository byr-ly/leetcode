package com.eb.bi.rs.opus.itemcf.predictscore;

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
 * 计算每个动漫的TopN相似动漫
 */
public class TopSimilarOpusReducer extends
		Reducer<Text, Text, Text, FloatWritable> {

	private int topN = 0;

	/*
	 * key:源动漫 value 格式：目的动漫|相似度
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashMap<String, Float> simMap = new HashMap<String, Float>();
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			simMap.put(fields[0], Float.parseFloat(fields[1]));
		}
		List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>(
				simMap.entrySet());
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>() {

			@Override
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		int count = 0;
		for (int i = 0; i < infoIds.size() && count < topN; i++) {
			context.write(new Text(key.toString() + "|"
					+ infoIds.get(i).getKey()), new FloatWritable(infoIds
					.get(i).getValue()));
			count++;
		}
	}

	@Override
	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		topN = Integer.parseInt(conf.get("conf.top.similarity.num"));
	}
}
