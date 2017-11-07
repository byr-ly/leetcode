package com.eb.bi.rs.mrasstorm.unifyrec.realtimerec.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * 工具类
 * 
 * @author ynn
 * @date 创建时间：2015-10-29 上午10:27:47
 * @version 1.0
 */
public class MergeUtils {

	/*
	 * 按value从大到小对Map进行排序,并取topN
	 */
	public static Map<String, Float> sortMapByValue(Map<String, Float> oriMap,
			int topN) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (oriMap != null && !oriMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(
					oriMap.entrySet());
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

	public static Map<String, Float> sortMapByValue(Map<String, Float> oriMap) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (oriMap != null && !oriMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(
					oriMap.entrySet());
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

	public static Map<String, Float> sortMapByValue(Map<String, Float> oriMap,
			float sumWeight, int count) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (oriMap != null && !oriMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(
					oriMap.entrySet());
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
			float sum = 0;
			int c = 0;
			while (iter.hasNext() && sum < sumWeight && c < count) {
				tmpEntry = iter.next();
				sum += tmpEntry.getValue();
				c++;
				sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
			}
		}
		return sortedMap;
	}

	public static Map<String, Float> sortByWeight(Map<String, Float> oriMap,
			Map<String, String> bookClassMap, int topN, float orderFactor) {
		Map<String, Float> orderedMap = new HashMap<String, Float>();
		orderedMap = sortMapByValue(oriMap);

		Map<String, Integer> clsMap = new HashMap<String, Integer>();
		Set<String> keySet = orderedMap.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String book = it.next();
			float score = orderedMap.get(book);
			String cls = bookClassMap.get(book);
			if (cls == null) {
				orderedMap.put(book, score);
			} else {
				if (clsMap.keySet().contains(cls)) {
					int count = clsMap.get(cls);
					orderedMap.put(book, (score - count * orderFactor));
					count++;
					clsMap.put(cls, count);
				} else {
					clsMap.put(cls, 1);
					orderedMap.put(book, score);
				}
			}
		}
		orderedMap = sortMapByValue(orderedMap, topN);
		return orderedMap;
	}

	public static Map<String, Float> getClassTopCountWeight(
			Map<String, Map<String, Float>> classBooksMap, int topN) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (classBooksMap != null && !classBooksMap.isEmpty()) {
			List<Map.Entry<String, Map<String, Float>>> entryList = new ArrayList<Map.Entry<String, Map<String, Float>>>(
					classBooksMap.entrySet());
			Collections.sort(entryList,
					new Comparator<Map.Entry<String, Map<String, Float>>>() {
						@Override
						public int compare(
								Entry<String, Map<String, Float>> entry1,
								Entry<String, Map<String, Float>> entry2) {
							float value1 = 0, value2 = 0;
							try {
								value1 = entry1.getValue().size();
								value2 = entry2.getValue().size();
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
			Iterator<Map.Entry<String, Map<String, Float>>> iter = entryList
					.iterator();
			Map.Entry<String, Map<String, Float>> tmpEntry = null;
			int count = 0;
			while (iter.hasNext()) {
				tmpEntry = iter.next();
				Map<String, Float> map = sortMapByValue(tmpEntry.getValue(), 10);
				sortedMap.putAll(map);
				count++;
				if (count == topN) {
					break;
				}
			}
		}
		return sortedMap;
	}

	// 热门分类即推荐中图书最多的top20分类
	public static Set<String> getHotClass(
			Map<String, ArrayList<String>> classFillerMap, int topN) {
		Set<String> topClass = new LinkedHashSet<String>();
		if (classFillerMap != null && !classFillerMap.isEmpty()) {
			List<Map.Entry<String, ArrayList<String>>> entryList = new ArrayList<Map.Entry<String, ArrayList<String>>>(
					classFillerMap.entrySet());
			Collections.sort(entryList,
					new Comparator<Map.Entry<String, ArrayList<String>>>() {
						@Override
						public int compare(
								Entry<String, ArrayList<String>> entry1,
								Entry<String, ArrayList<String>> entry2) {
							float value1 = 0, value2 = 0;
							try {
								value1 = entry1.getValue().size();
								value2 = entry2.getValue().size();
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

			for (int i = 0; i < entryList.size() && i < topN; i++) {
				topClass.add(entryList.get(i).getKey());
			}
		}
		return topClass;
	}
}
