package com.eb.bi.rs.frame2.evaluation.offline.basicindicators;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by houmaozheng on 2016/12/21.
 */
public class UserCountInfoReducer extends Reducer<Text, Text, Text, NullWritable>{

    public static enum FileRecorder {
        totalCount, //保留且推荐用户图书
        totalRetain, //保留用户图书
        totalRec //推荐用户图书
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int mark = 0;//用于判断，保留且推荐的用户图书 标识
        for (Text pair : values) {
            if (pair.toString().equals("retain")) {// 保留用户图书
                context.getCounter(FileRecorder.totalRetain).increment(1);
                mark += 1;//
            }
            if (pair.toString().equals("rec")) {// 推荐用户图书
                context.getCounter(FileRecorder.totalRec).increment(1);
                mark += 1;
            }
        }

        if ( mark >= 2 ) {//保留且推荐用户图书
            context.getCounter(FileRecorder.totalCount).increment(1);
        }
    }
}
