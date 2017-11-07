package com.eb.bi.rs.mras2.generec.gene_pref;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/6/27.
 * 计算用户对各基因的偏好程度
 * 输入：用户对各分类的偏好
 * 字段：用户|第一分类偏好|强度|第二分类偏好|强度|第三分类偏好|强度|相似分类|相似分类程度|
 * 历史阅读分类|阅读分类程度
 */
public class GenePrefMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 11) {
            StringBuffer s = new StringBuffer();
            for (int i = 1; i < 11; i++) {
                s.append(line[i] + "|");
            }
            s.deleteCharAt(s.length() - 1);
            context.write(new Text(line[0]), new Text(s.toString()));
        }
    }
}
