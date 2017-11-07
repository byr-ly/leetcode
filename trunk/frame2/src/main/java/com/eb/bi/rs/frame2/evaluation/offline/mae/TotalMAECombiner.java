package com.eb.bi.rs.frame2.evaluation.offline.mae;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 2016/12/23.
 */
public class TotalMAECombiner extends Reducer<Text,Text,Text,Text> {

    // Reduce Method
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        double eSum = 0;
        for (Text pair : values) {
            if (key.toString().equals("total")) {// 所有用户
                count++;
                eSum += Float.parseFloat(pair.toString());
            }
        }

        String output = count + "|" + eSum;
        context.write(new Text("total"), new Text(output));
    }
}
