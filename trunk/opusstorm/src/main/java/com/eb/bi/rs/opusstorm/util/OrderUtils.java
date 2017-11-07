package com.eb.bi.rs.opusstorm.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OrderUtils {

	public static Map<String, Float> sortByValue(
			Map<String, Float> opusScoreMap, int topN) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (opusScoreMap != null && !opusScoreMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(
					opusScoreMap.entrySet());
			Collections.sort(entryList,
					new Comparator<Map.Entry<String, Float>>() {
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
			int count = 0;
			while (iter.hasNext()) {
				tmpEntry = iter.next();
				sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
				count++;
				if (count == topN) {
					break;
				}
			}
		}
		return sortedMap;
	}

}
