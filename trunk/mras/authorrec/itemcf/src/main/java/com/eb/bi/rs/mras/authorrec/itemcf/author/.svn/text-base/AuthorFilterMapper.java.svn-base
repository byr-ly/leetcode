package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AuthorFilterMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {
    /**
     * @param value: 用户-作者打分表
     *               格式：msisdn|authorid|score
     *               只筛选推荐库中图书对应且打分>0的作者们
     *               map out:
     *               key:authorid; value:msisdn|score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[1];
        String scorestr = strs[2];
        double score = Double.parseDouble(scorestr);
        context.write(new Text(authorid), new DoubleWritable(score));
    }
}
