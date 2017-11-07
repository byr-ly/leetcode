package com.eb.bi.rs.frame2.algorithm.tagBasedRec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhengyaolin on 2016/12/5
 *
 * Description：recommend topN
 *
 * Input：
 *      item|user
 *
 * Output：
 *      key：user
 *      value：<item, flag:history>
 *
 */
public class HistoryFilterMapper extends Mapper<Object, Text, Text, TextPair> {
    private Text resultKey = new Text();
    private TextPair resultValue = new TextPair();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = new String(value.toString());
        //drop null
        if (line == null || line.equals("")){
            return;
        }
        String[] split = line.split("\\|");

        resultKey.set(split[1]);
        resultValue.set(split[0], "history");

        context.write(resultKey, resultValue);
    }
}
