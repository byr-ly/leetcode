package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**求忠诚度和流行度最大最小值
 * Created by linwanying on 2016/11/15.
 */
public class PeakReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double loyaltymax = 0, loyaltymin = 1, popularitymax = 0, popularitymin = 1;
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            double loyalty = Double.parseDouble(fields[0]);
            double popularity = Double.parseDouble(fields[1]);
            loyaltymax = Math.max(loyalty, loyaltymax);
            loyaltymin = Math.min(loyalty, loyaltymin);
            popularitymax = Math.max(popularity, popularitymax);
            popularitymin = Math.min(popularity, popularitymin);
        }
        context.write(new Text(loyaltymax + "|" + loyaltymin + "|" + popularitymax + "|" + popularitymin),
                NullWritable.get());
    }
}
