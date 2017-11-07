package com.eb.bi.rs.frame2.service.dataload.dna2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//这个是传统的MR导入数据到hbase的程序，先在map端进行分割数据，做一下初步的处理，然后在reduce端把数据插入到hbase里
public class DnaChaptCnt2HbaseMapper extends Mapper<Object, Text, Text, Text> {
    private String split;

    public void map(Object o, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(split, -1);
        context.write(new Text(items[0]), new Text(items[1]));
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        split = conf.get("conf.text.split");
    }
}
