package com.eb.bi.rs.andedu.synthesize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 输入数据格式：
 * 			 key:属性组合1 
 * 			 value:资源A|品质分 
 * 输出数据格式：
 * 			 key:属性组合;资源id1,score|资源id2,score|...
 * 			 value: NullWritable
 */
public class SynthesizeSortReducer extends
		Reducer<Text, Text, Text, NullWritable> {
	private int count;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Float> resAndScoreMap = new HashMap<String, Float>();

		for (Text val : values) {
			String[] split = val.toString().split("\\|", -1);
			if (split.length == 2) {
				resAndScoreMap.put(split[0], Float.parseFloat(split[1]));
			}
		}
		StringBuilder sb = new StringBuilder("");
		Map<String, Float> sortMapByValue = sortMapByValue(resAndScoreMap);

		int countNum = 0;
		for (Map.Entry<String, Float> mapping : sortMapByValue.entrySet()) {
			sb.append(mapping.getKey() + "," + mapping.getValue() + "|");
			countNum++;
			if (countNum == count && key.toString().equals("all+all+all+all+all+all+all")) {
				String filler = sb.toString().substring(0,
						sb.toString().length() - 1);
				context.write(new Text("res_filler" + ";" + filler),
						NullWritable.get());
			}
		}
		String result = sb.toString().substring(0, sb.toString().length() - 1);

		context.write(new Text(key.toString() + ";" + result),
				NullWritable.get());
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration configuration = context.getConfiguration();
		count = configuration.getInt("count.num", 100);
	}

	//map按value排序
	public Map<String, Float> sortMapByValue(Map<String, Float> oriMap) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (oriMap != null && !oriMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(oriMap.entrySet());
			Collections.sort(entryList,
					new Comparator<Map.Entry<String, Float>>() {
						@Override
						public int compare(Entry<String, Float> entry1,
								Entry<String, Float> entry2) {
							float value1 = 0, value2 = 0;
							try {
								value1 = entry1.getValue();
								value2 = entry2.getValue();
							} catch (NumberFormatException e) {
								value1 = 0;
								value2 = 0;
							}
							if (value2 > value1) {
								return 1;
							} else if (value2 < value1) {
								return -1;
							} else if ((value2 - value1) < 0.001) {
								return 0;
							} else {
								return 0;
							}

						}
					});
			Iterator<Map.Entry<String, Float>> iter = entryList.iterator();
			Map.Entry<String, Float> tmpEntry = null;
			while (iter.hasNext()) {
				tmpEntry = iter.next();
				sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
			}
		}
		return sortedMap;
	}

}
