package com.eb.bi.rs.mras2.andnewsrec.news_word_transpose;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LiMingji on 2016/3/18.
 */
public class NewsWordsTransposeReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * 输入数据为：
     * KEY:word|classID
     * VALUE:newsID|weight
     * 输出数据为:
     * word|classID   newsID,newsID,score
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> newsScoreMap = new HashMap<String, Double>();

        for (Text value : values) {
            String[] fields = value.toString().split("\\|", -1);
            if (fields.length < 2) {
                System.out.println("BadLine !!!" + value);
                return;
            }
            String newsID = fields[0];
            Double scores = 0.0;
            try {
                scores = Double.parseDouble(fields[1]);
            } catch (NumberFormatException e) {
                continue;
            }
            newsScoreMap.put(newsID, scores);
        }

        DecimalFormat dcm = new DecimalFormat("0.######");
        for (Map.Entry<String, Double> entryOutter : newsScoreMap.entrySet()) {
            for (Map.Entry<String, Double> entryInner : newsScoreMap.entrySet()) {
                if (entryOutter.getKey().equals(entryInner.getKey())) {
                    continue;
                }
                double weight = entryInner.getValue() * entryOutter.getValue();
                if (weight < 1e-6) {
                    continue;
                }
                if (entryOutter.getKey().compareTo(entryInner.getKey()) < 0) {
                    continue;
                }
                context.write(key, new Text(entryInner.getKey() + "|" + entryOutter.getKey() + "|" + dcm.format(entryInner.getValue()) + "|" + dcm.format(entryOutter.getValue())));
                context.write(key, new Text(entryOutter.getKey() + "|" + entryInner.getKey() + "|" + dcm.format(entryInner.getValue()) + "|" + dcm.format(entryOutter.getValue())));
            }
        }
    }
}
