package com.eb.bi.rs.frame2.evaluation.online.recAvgOLmtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by houmaozheng on 2016/12/23.
 */
public class RecAvgOLMtxCombiner extends Reducer<Text,Text,Text,Text> {

    private String fieldDelimiter;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    // Reduce Method
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        //用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU
        double CTRuSum = 0;
        double OTRuSum = 0;
        double ConUSum = 0;

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 3) {
                continue;
            }

            count ++;
            CTRuSum += Float.parseFloat(fields[0]);
            OTRuSum += Float.parseFloat(fields[1]);
            ConUSum += Float.parseFloat(fields[2]);
        }

        String output = count + "|" + CTRuSum + "|" + OTRuSum + "|" + ConUSum;
        context.write( key, new Text(output));
    }
}
