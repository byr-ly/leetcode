package com.eb.bi.rs.mras2.generec.category_pref;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/6/27
 * 计算用户的分类偏好（只需要前面8个字段）
 * 输入：用户历史偏好表  DMN.IRECM_US_PREF_ALL
 * 字段：msisdn用户|group_type用户群(深度：2;中度：3;浅度：4)|class1_id第一分类偏好|
 * class1_value强度|class2_id第二分类偏好|class2_value强度|class3_id第三分类偏好|
 * class3_value强度
 *
 * //用户 | | | 前三分类 | 相似分类 | 历史阅读分类
 */
public class CategoryPrefMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 6) {
            context.write(new Text(line[0]), new Text(value.toString()));
        }
    }
}
