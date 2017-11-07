package com.eb.bi.rs.andedu.itemcf.predictscore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 取用户预测TopN评分的物品
 */
public class TopUserPredictItemReducer extends Reducer<Text, Text, Text, Text> {

	int topN = 0;

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Map<String, Float> map = new HashMap<String, Float>();
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			map.put(fields[0], Float.parseFloat(fields[1]));
		}

		List<Map.Entry<String, Float>> scores = new ArrayList<Map.Entry<String, Float>>(
				map.entrySet());
		Collections.sort(scores, new Comparator<Map.Entry<String, Float>>() {

			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		int count = 0;
		String objScore = "";
		for (int i = 0; i < scores.size() && count < topN; i++) {
			objScore += scores.get(i).getKey() + "," + scores.get(i).getValue()
					+ "|";
			count++;
		}
		context.write(key, new Text(objScore));
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		topN = Integer.parseInt(conf.get("conf.top.userpredictscore.num", "50"));
	}
}
