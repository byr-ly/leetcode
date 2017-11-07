package com.eb.bi.rs.andedu.inforec.get_infos_num;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liyang on 2016/5/31.
 */
public class GetInfosNumReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    public void reduce(Text key,Iterable<IntWritable> values,Context context)
        throws IOException,InterruptedException{
        int sum = 0;
        for(IntWritable val:values){
            sum += val.get();
        }
        context.write(new Text("newsNum" + "|" + String.valueOf(sum)),NullWritable.get());
    }
}
