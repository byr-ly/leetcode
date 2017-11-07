package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RecommResultMergeMapper
        extends Mapper<Object, Text, Text, RecommItemWritable> {
    /**
     * @param value: 已读推荐结果表、预测推荐结果表、补白推荐结果表
     *               格式：msisdn|authorid|bookid|score|type
     *               map out:
     *               格式：key: ; value:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorid = strs[1];
        String bookid = strs[2];
        double score = Double.parseDouble(strs[3]);
        int type = Integer.parseInt(strs[4]);
        RecommendItem item = new RecommendItem(authorid, bookid, score);
        RecommItemWritable writable = new RecommItemWritable(item, type);
        context.write(new Text(msisdn), writable);
    }
}
