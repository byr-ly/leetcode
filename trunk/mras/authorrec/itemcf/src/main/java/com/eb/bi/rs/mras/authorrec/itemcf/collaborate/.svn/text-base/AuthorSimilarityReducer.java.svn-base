package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.SimilarityWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 计算两作者之间的相似度
 */
public class AuthorSimilarityReducer
        extends Reducer<Text, SimilarityWritable, Text, DoubleWritable> {
    private int authorCommUserMin = 10;
    private float authorSimMin = 0f;

    @Override
    public void reduce(Text key, Iterable<SimilarityWritable> values,
                       Context context) throws IOException, InterruptedException {
        int cnt = 0;
        float sumW = 0.0f;
        float sumI = 0.0f;
        float sumJ = 0.0f;
        for (SimilarityWritable value : values) {
            cnt++;
            sumW += value.getWr();
            sumI += value.getRi();
            sumJ += value.getRj();
        }
        if (cnt <= authorCommUserMin) {
            return;
        }
        double deno = Math.sqrt(sumI) * Math.sqrt(sumJ);
        if (sumW > 0.0 && deno > 0.0) {
            double sim = sumW / deno;
            if (sim < authorSimMin) {
                //			return;
            }
            context.write(key, new DoubleWritable(sim));
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        authorCommUserMin = context.getConfiguration().getInt(
                PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY, authorCommUserMin);
        String strlog = String.format("min comm user %d", authorCommUserMin);
        System.out.println(strlog);
        authorSimMin = context.getConfiguration().getFloat(
                PluginUtil.COLL_AUTHOR_SIM_MIN, authorSimMin);
    }
}

