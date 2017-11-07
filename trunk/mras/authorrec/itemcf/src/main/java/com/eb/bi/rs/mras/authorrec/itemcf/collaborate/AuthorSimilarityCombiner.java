package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.SimilarityWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorSimilarityCombiner
        extends Reducer<Text, SimilarityWritable, Text, SimilarityWritable> {
    public void reduce(Text key, Iterable<SimilarityWritable> values, Context context)
            throws IOException, InterruptedException {
        ///////////
        //还要增加count，不然会影响reducer结果
        float sumW = 0.0f;
        float sumI = 0.0f;
        float sumJ = 0.0f;
        for (SimilarityWritable value : values) {
            sumW += value.getWr();
            sumI += value.getRi();
            sumJ += value.getRj();
        }
        SimilarityWritable writable = new SimilarityWritable(sumW, sumI, sumJ);
        context.write(key, writable);
    }
}
