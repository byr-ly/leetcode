package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.data2libsvm;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**数据预处理mapper
 * Created by linwanying on 2017/4/17.
 */
public class dataPrepMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 5) {
            return;
        }
        StringBuilder keyout = new StringBuilder(fields[0]);
        for (int i = 5; i < fields.length-1; ++i) {
            if (fields[i].isEmpty()) continue;
            keyout.append(" ").append(String.valueOf(i-4)).append(":").append(fields[i]);
        }
        keyOut.set(keyout.toString());
        context.write(keyOut, valueOut);
    }
}
