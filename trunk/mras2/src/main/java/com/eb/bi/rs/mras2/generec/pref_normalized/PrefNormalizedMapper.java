package com.eb.bi.rs.mras2.generec.pref_normalized;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/6/27.
 * 对基因偏好进行归一化
 * 输入：用户对各基因的偏好程度
 * 字段：用户|基因|程度
 */
public class PrefNormalizedMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 3) {
            context.write(new Text(line[0]), new Text(line[1] + "|" + line[2]));
        }
    }
}
