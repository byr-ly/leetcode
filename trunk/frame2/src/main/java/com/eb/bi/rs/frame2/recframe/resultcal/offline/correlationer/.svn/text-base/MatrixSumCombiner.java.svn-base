package com.eb.bi.rs.frame2.recframe.resultcal.offline.correlationer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixSumCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float scoreSum = 0;

        for (Text value : values) {
            scoreSum += Float.valueOf(value.toString());
        }

        //String[] keys = key.toString().split("\\???");//�ָ����
        context.write(new Text(key.toString()), new Text(scoreSum + ""));
    }
}
