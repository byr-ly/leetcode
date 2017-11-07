package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RandomSelectorMapper extends Mapper<Object, Text, Text, Text> {
    private String fieldDelimiter;
    private int keyFieldIdx;

    @Override
    protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String[] fields = value.toString().split(fieldDelimiter);
        context.write(new Text(fields[keyFieldIdx]), value);
    }

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
        keyFieldIdx = conf.getInt("key.field.index", 0);
    }

}
