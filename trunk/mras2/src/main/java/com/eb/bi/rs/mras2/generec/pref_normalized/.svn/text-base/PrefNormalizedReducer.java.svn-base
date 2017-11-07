package com.eb.bi.rs.mras2.generec.pref_normalized;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/6/27.
 */
public class PrefNormalizedReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, Double> genePrefMap = new HashMap<String, Double>();
        double sum = 0.0;
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            double score = Double.parseDouble(line[1]);
            genePrefMap.put(line[0], score);
            sum += score;
        }

        StringBuffer s = new StringBuffer();
        Iterator iter = genePrefMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String gene = (String) entry.getKey();
            double score = genePrefMap.get(gene);
            double result = score / sum;
            s.append(gene + "," + result + "|");
        }
        s.deleteCharAt(s.length() - 1);
        context.write(new Text(key.toString()), new Text(s.toString()));
    }
}

