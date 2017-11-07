package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NoFillerUserFilterReducer
        extends Reducer<Text, IntWritable, Text, NullWritable> {
    private int recommAuthorNum = 9;

    /**
     * reduce out: key:msisdn
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        if (sum < recommAuthorNum) {
            return;
        }
        context.write(key, NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        recommAuthorNum = context.getConfiguration().getInt(
                PluginUtil.RECOMM_AUTHOR_NUM, recommAuthorNum);
    }
}
