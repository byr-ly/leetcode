package com.eb.bi.rs.mras.authorrec.itemcf.bookinfo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorBookCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 作者-分类-计数
     * reduce out: key:authorid|classid; value:count
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
