package com.eb.bi.rs.mras2.booklistrec.sheet_filter_hbase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/8/22.
 */
public class SheetFilterHbaseMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length == 2) {
            context.write(new Text(line[0]), new Text("1|" + value.toString()));
        } else if (line.length >= 5) {
            context.write(new Text(line[0]), new Text("2|" + value.toString()));
        }
    }
}
