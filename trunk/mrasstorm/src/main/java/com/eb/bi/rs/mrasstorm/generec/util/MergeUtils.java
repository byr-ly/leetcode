package com.eb.bi.rs.mrasstorm.generec.util;


import java.util.*;
import java.util.Map.Entry;

/**
 * 工具类
 *
 * @author ynn
 * @version 1.0
 * @date 创建时间：2015-10-29 上午10:27:47
 */
public class MergeUtils {

    /*
     * 按value从大到小对Map进行排序,并取topN
     */
    public static Map<String, Float> sortMapByValue(Map<String, Float> oriMap,
                                                    int topN) {
        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        if (oriMap != null && !oriMap.isEmpty()) {
            List<Entry<String, Float>> entryList = new ArrayList<Entry<String, Float>>(
                    oriMap.entrySet());
            Collections.sort(entryList,
                    new Comparator<Entry<String, Float>>() {
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
            Iterator<Entry<String, Float>> iter = entryList.iterator();
            Entry<String, Float> tmpEntry = null;
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
            List<Entry<String, Float>> entryList = new ArrayList<Entry<String, Float>>(
                    oriMap.entrySet());
            Collections.sort(entryList,
                    new Comparator<Entry<String, Float>>() {
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
            Iterator<Entry<String, Float>> iter = entryList.iterator();
            Entry<String, Float> tmpEntry = null;
            while (iter.hasNext()) {
                tmpEntry = iter.next();
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
}
