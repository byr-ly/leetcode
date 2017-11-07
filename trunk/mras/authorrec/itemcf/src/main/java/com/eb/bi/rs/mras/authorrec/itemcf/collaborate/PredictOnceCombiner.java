package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.PredictWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PredictOnceCombiner
        extends Reducer<Text, PredictWritable, Text, PredictWritable> {
    public void reduce(Text key, Iterable<PredictWritable> values, Context context)
            throws IOException, InterruptedException {
        float simSum = 0;
        float conSum = 0;
        for (PredictWritable value : values) {
            float sim = value.getSim();
            simSum += sim;
            conSum += value.getCon();
        }
        if (simSum == 0) {
            return;
        }
        PredictWritable writable = new PredictWritable(simSum, conSum);
        context.write(key, writable);
    }
}
