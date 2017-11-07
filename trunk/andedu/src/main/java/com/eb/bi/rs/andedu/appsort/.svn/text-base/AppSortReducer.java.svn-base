package com.eb.bi.rs.andedu.appsort;

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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 输入数据格式：
 * 			 key:key 
 * 			 value:资源id|score 
 * 输出数据格式：
 * 			 key:key;资源id1,score|资源id2,score|...
 * 			 value: NullWritable
 */
public class AppSortReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder("");
		Map<String, Float> appAndScoreMap = new HashMap<String, Float>();
		String score = "";
		for (Text val : values) {
			String[] split = val.toString().split("\\|", -1);
			score = StringUtils.isBlank(split[1]) ? "0.00" : split[1] ;
			if (split.length == 2) {
				appAndScoreMap.put(split[0], Float.parseFloat(score));
			}
		}
		Map<String, Float> afterSortMap = sortMapByValue(appAndScoreMap);
		
		for (Entry<String, Float> entry : afterSortMap.entrySet()) {
			String appId = entry.getKey();
			Float appScore = entry.getValue();
			
			sb.append(appId+","+appScore+"|");
		}

		String result = sb.toString().substring(0, sb.toString().length() - 1);

		context.write(new Text(key.toString() + ";" + result),
				NullWritable.get());
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
