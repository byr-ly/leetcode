package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**figure out sum of indexes
 * Created by linwanying on 2016/12/2.
 */
public class SumReducer extends Reducer<Text, Text, Text, Text> {
    private int action_num;
    double[] arrSum;
    @Override
    public void setup(Context context) throws IOException,InterruptedException {
        action_num = Integer.valueOf(context.getConfiguration().get("action_num"));
        arrSum = new double[action_num];
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] data = value.toString().split("\\|");
            for (int i = 0; i < action_num; ++i) {
                arrSum[i] += Double.valueOf(data[i]);
            }
        }
        String keyOut = "";
        for (int i = 0; i < action_num; ++i) {
            keyOut += String.valueOf(arrSum[i]) + "|";
        }
        context.write(new Text(key), new Text(keyOut));
    }
}