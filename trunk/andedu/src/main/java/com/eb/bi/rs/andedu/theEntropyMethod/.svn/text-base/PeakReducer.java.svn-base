package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**find the maximum and minimum of indexes
 * Created by linwanying on 2016/12/1.
 */
public class PeakReducer extends Reducer<Text, Text, Text, Text> {
    private int action_num;
    private double[] arrMax;
    private double[] arrMin;
    @Override
    public void setup(Context context) throws IOException,InterruptedException {
        action_num = Integer.valueOf(context.getConfiguration().get("action_num"));
        arrMax = new double[action_num];
        arrMin = new double[action_num];
        for (int i = 0; i < action_num; ++i) {
            arrMin[i] = Integer.MAX_VALUE;
        }
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields.length != 2 * action_num) return;
            for (int i = 0; i < action_num; ++i) {
                arrMax[i] = Math.max(arrMax[i], Double.valueOf(fields[i]));
                arrMin[i] = Math.min(arrMin[i], Double.valueOf(fields[i+3]));
            }
        }
        String result = "";
        for (int i = 0; i < action_num; ++i) {
            result += arrMax[i] + "|";
        }
        for (int i = 0; i < action_num; ++i) {
            result += arrMin[i] + "|";
        }
        context.write(new Text(key), new Text(result));
    }
}
