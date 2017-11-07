package com.eb.bi.rs.frame2.recframe.resultcal.offline.supplementer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Supplement1ToberecResultMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text("0|" + value.toString()));
    }
}
