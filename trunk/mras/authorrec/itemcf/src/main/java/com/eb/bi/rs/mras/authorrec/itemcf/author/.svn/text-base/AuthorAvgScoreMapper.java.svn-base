package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AuthorAvgScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    /**
     * @param value: 筛选出推荐库关联作者后的用户-作者打分表
     *               格式：msisdn|authorid|score
     *               map out:
     *               key:authorid; value:score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[1];
        String scoreStr = strs[2];
        double score = Double.parseDouble(scoreStr);
        context.write(new Text(authorid), new DoubleWritable(score));
    }
}
