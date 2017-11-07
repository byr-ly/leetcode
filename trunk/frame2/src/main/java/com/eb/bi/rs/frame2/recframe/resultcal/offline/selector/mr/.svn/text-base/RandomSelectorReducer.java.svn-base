package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class RandomSelectorReducer extends Reducer<Text, Text, Text, Text> {

    private String fieldDelimiter;
    private int randBaseFieldIdx;
    private int itemFieldIdx;
    private int selectNum;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

        Random random = new Random(new Date().getTime());
        HashMap<String, ArrayList<String>> baseItemMap = new HashMap<String, ArrayList<String>>();
        for (Text value : values) {
            String[] fields = value.toString().split(fieldDelimiter);
            if (baseItemMap.containsKey(fields[randBaseFieldIdx])) {/**/
                baseItemMap.get(fields[randBaseFieldIdx]).add(fields[itemFieldIdx]);
            } else {
                ArrayList<String> array = new ArrayList<String>();
                array.add(fields[itemFieldIdx]);
                baseItemMap.put(fields[randBaseFieldIdx], array);
            }
        }

        int count = 0;
        int quotient = selectNum / baseItemMap.size();
        while (quotient > 0) {
            Iterator<String> baseFieldIter = baseItemMap.keySet().iterator();
            while (baseFieldIter.hasNext()) {
                String baseField = baseFieldIter.next();
                ArrayList<String> itemArray = baseItemMap.get(baseField);
                String item = itemArray.get(random.nextInt(itemArray.size()));
                //写结果
                context.write(key, new Text(item));
                ++count;
                itemArray.remove(item);
                if (itemArray.size() == 0) {
                    baseFieldIter.remove();/*在遍历容器的时候，如果要删除容器中的元素，要使用迭代器删除*/
                }
            }

            if (baseItemMap.size() == 0) {
                return;
            } else {
                quotient = (selectNum - count) / baseItemMap.size();
            }
        }

        int remainder = selectNum - count;
        HashSet<Integer> picked = new HashSet<Integer>();
        for (int i = 0; i < remainder; i++) {/*有不够的情况，是否需要配置多条路径，一条是不需要补白的，一条是需要补白的，一条是全部的*/
            Object[] baseFieldArray = baseItemMap.keySet().toArray();
            int idx = random.nextInt(baseFieldArray.length);
            while (picked.contains(idx)) {
                idx = random.nextInt(baseFieldArray.length);
            }
            picked.add(idx);
            ArrayList<String> itemArray = baseItemMap.get(baseFieldArray[idx]);
            String item = itemArray.get(random.nextInt(itemArray.size()));
            context.write(key, new Text(item));
        }
    }

    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
        randBaseFieldIdx = conf.getInt("random.base.field.index", 0);
        selectNum = conf.getInt("select.number", 1);
    }

}
