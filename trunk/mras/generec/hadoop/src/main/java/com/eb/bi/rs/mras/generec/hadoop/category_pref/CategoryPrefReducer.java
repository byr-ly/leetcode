package com.eb.bi.rs.mras.generec.hadoop.category_pref;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/6/27.
 */
public class CategoryPrefReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //group_type用户群(深度：2;中度：3;浅度：4)|class1_id第一分类偏好|class1_value强度
        // |class2_id第二分类偏好|class2_value强度|class3_id第三分类偏好|class3_value强度

        HashMap<String, String> similarTypeMap = new HashMap<String, String>();
        HashMap<String, String> readTypeMap = new HashMap<String, String>();
        HashMap<String, String> userPrefMap = new HashMap<String, String>();

        //用户 | | | 前三分类 | 相似分类 | 历史阅读分类
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (line.length == 6) {
                String userid = line[0];
                String similarType = line[4];
                String readType = line[5];
                similarTypeMap.put(userid, similarType);
                readTypeMap.put(userid, readType);
            }

            if (line.length >= 8) {
                String userid = line[0];
                userPrefMap.put(userid, val.toString());
            }
        }

        Iterator<Map.Entry<String, String>> iterator = userPrefMap
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String user = entry.getKey();
            String value = userPrefMap.get(user);
            String[] line = value.split("\\|");
            String userType = line[1];
            String class1 = line[2];
            String class2 = line[4];
            String class3 = line[6];
            String class1_value = line[3];
            String class2_value = line[5];
            String class3_value = line[7];

            double class1_dValue, class2_dValue, class3_dValue, sumValue;
            double similarValue = 0.0;
            double readValue = 0.0;
            int count = 0;
            if (class1_value.isEmpty()) {
                class1_dValue = 0.0;
            } else {
                class1_dValue = Double.parseDouble(class1_value);
                count++;
            }
            if (class2_value.isEmpty()) {
                class2_dValue = 0.0;
            } else {
                class2_dValue = Double.parseDouble(class2_value);
                count++;
            }
            if (class3_value.isEmpty()) {
                class3_dValue = 0.0;
            } else {
                class3_dValue = Double.parseDouble(class3_value);
                count++;
            }
            sumValue = class1_dValue + class2_dValue + class3_dValue;

            if (((userType.equals("2") || userType.equals("3")) && sumValue >= 0.5)
                    || ((userType.equals("4") && sumValue >= 0.9))) {
                if (count == 0) similarValue = 0.0;
                else similarValue = 0.8 * sumValue / count;
                readValue = 0.3 * class3_dValue;
            } else {
                if (count == 0) similarValue = 0.0;
                else similarValue = sumValue / count;
                if (userType.equals("2") || userType.equals("3")) {
                    readValue = class3_dValue;
                } else if (userType.equals("4")) {
                    readValue = class2_dValue;
                }
            }

            String similarType = "";
            String readType = "";
            if (similarTypeMap.containsKey(key.toString()) && readTypeMap.containsKey(key.toString())) {
                similarType += similarTypeMap.get(key.toString());
                readType += readTypeMap.get(key.toString());
            } else if (!similarTypeMap.containsKey(key.toString()) && readTypeMap.containsKey(key.toString())) {
                similarValue = 0.0;
                readType += readTypeMap.get(key.toString());
            } else if (similarTypeMap.containsKey(key.toString()) && !readTypeMap.containsKey(key.toString())) {
                similarType += similarTypeMap.get(key.toString());
                readValue = 0.0;
            } else {
                similarValue = 0.0;
                readValue = 0.0;
            }

            String result = class1 + "|" + class1_value + "|" + class2 + "|" + class2_value +
                    "|" + class3 + "|" + class3_value + "|" + similarType + "|" + similarValue +
                    "|" + readType + "|" + readValue;
            context.write(new Text(key.toString() + "|" + result), NullWritable.get());
        }
    }
}

