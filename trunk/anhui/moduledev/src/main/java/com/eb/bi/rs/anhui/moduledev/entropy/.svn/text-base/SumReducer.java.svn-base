package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**对用户行为每列求和
 * Created by linwanying on 2016/11/16.
 */
public class SumReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double  loyalSum = 0, popSum = 0;
        long changeSum = 0, size = 0;
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            changeSum += Integer.parseInt(fields[0]);
            loyalSum += Double.parseDouble(fields[1]);
            popSum += Double.parseDouble(fields[2]);
            ++size;
        }
        context.write(new Text(String.valueOf(changeSum) + "|" + String.valueOf(loyalSum)
                + "|" + String.valueOf(popSum) + "|" + String.valueOf(size)), NullWritable.get());
    }
}
