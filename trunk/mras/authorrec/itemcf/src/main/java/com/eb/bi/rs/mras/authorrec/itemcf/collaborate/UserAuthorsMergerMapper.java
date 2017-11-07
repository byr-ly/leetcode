package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserAuthorsMergerMapper
        extends Mapper<Object, Text, Text, ScoreWritable> {
    /**
     * @param value: 筛选出推荐库关联作者后的用户-作者打分表
     *               格式：msisdn|authorid|score
     *               map out:
     *               key:msisdn; value:authorid|score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorid = strs[1];
        String scoreStr = strs[2];
        double score = Double.parseDouble(scoreStr);
        ScoreWritable scoreWritable = new ScoreWritable(authorid, score);
        context.write(new Text(msisdn), scoreWritable);
    }
}
