package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.SimilarityWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 计算单个用户对两本图书之间相似度的贡献
 */
public class AuthorSimilarityMapper
        extends Mapper<Object, Text, Text, SimilarityWritable> {

    /**
     * @param value 格式：authorI|authorJ|scoreI|scoreJ
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length != 4) {
            return;
        }
        String authoridI = fields[0];
        String authoridJ = fields[1];
        float scoreI = Float.parseFloat(fields[2]);
        float scoreJ = Float.parseFloat(fields[3]);
        float wr = scoreI * scoreJ;
        float ri = scoreI * scoreI;
        float rj = scoreJ * scoreJ;

        String keyOut = String.format("%s|%s", authoridI, authoridJ);
        context.write(new Text(keyOut), new SimilarityWritable(wr, ri, rj));
    }

}
