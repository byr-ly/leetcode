package com.eb.bi.rs.mras2.andnewsrec.get_news_num;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/5/31.
 */
public class GetNewsNumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key,Text value,Context context)
        throws IOException,InterruptedException{
        context.write(new Text(" "),new IntWritable(1));
    }
}
