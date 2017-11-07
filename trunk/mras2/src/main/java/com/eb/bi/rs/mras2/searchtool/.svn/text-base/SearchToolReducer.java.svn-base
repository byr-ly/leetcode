package com.eb.bi.rs.mras2.searchtool;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liyang on 2016/12/26.
 */
public class SearchToolReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text val : values){
            context.write(new Text(val),NullWritable.get());
        }
    }
}
