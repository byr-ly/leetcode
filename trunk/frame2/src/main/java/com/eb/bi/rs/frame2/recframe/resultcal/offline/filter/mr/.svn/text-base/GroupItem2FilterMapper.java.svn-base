package com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupItem2FilterMapper extends Mapper<Object, Text, TextPair, TextPair> {
    private int keyFieldIdx;
    private String fieldDelimiter;

    @Override
    protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String[] fields = value.toString().split(fieldDelimiter);
        context.write(new TextPair(fields[keyFieldIdx], "1"), new TextPair(value.toString(), "1"));
    }

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        keyFieldIdx = conf.getInt("to.filter.key.field.index", 0);
        fieldDelimiter = conf.get("to.filter.field.delimiter", "\\|");
    }
}
