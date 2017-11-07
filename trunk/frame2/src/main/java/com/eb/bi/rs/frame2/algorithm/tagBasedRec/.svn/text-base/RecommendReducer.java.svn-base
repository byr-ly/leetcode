package com.eb.bi.rs.frame2.algorithm.tagBasedRec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhengyaolin on 2016/12/5
 *
 * Description: Recommend topN
 *
 * Input:
 *      key: user
 *      value:
 *          1. <item, rating>
 *          2. <item, flag:history>
 *
 * Output:
 *      key: user|item1|item2|...|item100
 *      value: null
 *
 */
public class RecommendReducer extends Reducer<Text, TextPair, Text, NullWritable> {
    private Text resultKey = new Text();
    private int topN = 0;
    private boolean filter = true;

    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        topN = Integer.parseInt(conf.get("topN"));
        filter = conf.get("filter").equals("true") ? true : false;
    }

    public void reduce(Text key, Iterable<TextPair> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, Double> ratings = new HashMap<String, Double>();
        ArrayList<String> hisItems = new ArrayList<String>();
        for(TextPair val : values) {
            if (filter && val.getSecond().toString().equals("history")) {
                hisItems.add(val.getFirst().toString());
            } else {
                ratings.put(val.getFirst().toString(), Double.parseDouble(val.getSecond().toString()));
            }
        }

        // filter history
        if (filter) {
            for(String item : hisItems) {
                ratings.remove(item);
            }
        }

        List<Map.Entry<String, Double>> ratingList = new ArrayList<Map.Entry<String, Double>>(ratings.entrySet());
        // sorting descending
        Collections.sort(ratingList, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> r1, Map.Entry<String, Double> r2) {
                //return 1, if r1 < r2
                double tmp = r1.getValue() - r2.getValue();
                return (tmp < 0 ? 1 : (tmp == 0 ? 0 : -1));
            }
        });

        int sum = ratings.size();
        if (sum < topN) topN = sum;
        String result = key.toString();
        for (int i = 0; i < topN; i++) {
            result += "|" + ratingList.get(i).getKey();
        }
        resultKey.set(result);
        context.write(resultKey, NullWritable.get());
    }
}
