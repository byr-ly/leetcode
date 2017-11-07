package com.eb.bi.rs.frame2.evaluation.offline.mae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by houmaozheng on 2016/12/21.
 * 用于获取两份数据的交集
 * 输出交集结果
 * 字段：msisdn用户 | id 物品ID | score 实际打分 | rec_scroe 推荐打分
 */
public class UserScoreInfoReducer extends Reducer<Text, Text, Text, NullWritable>{

    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int mark = 0;//用于判断，保留且推荐的用户物品 标识
        float rec_scroe = 0;
        float retain_score = 0;

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);
            if ( fields[0].equals("rec") ) {
                rec_scroe = Float.parseFloat(fields[1]);
                mark++;
            }
            if ( fields[0].equals("retain") ) {
                retain_score = Float.parseFloat(fields[1]);
                mark++;
            }
        }

        if ( mark == 2 ) {//保留且推荐用户物品
            String output = key.toString() + "|" + retain_score + "|" + rec_scroe;
            context.write(new Text(output), NullWritable.get());
        }
    }
}
