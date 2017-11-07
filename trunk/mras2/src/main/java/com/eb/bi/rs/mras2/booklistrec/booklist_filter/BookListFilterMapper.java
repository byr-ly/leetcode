package com.eb.bi.rs.mras2.booklistrec.booklist_filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/4/28.
 */
public class BookListFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 4) {
            context.write(new Text(line[0]), new Text(line[1] + "|" + line[2] + "|" + line[3]));
        }
    }
}