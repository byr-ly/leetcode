package com.eb.bi.rs.frame2.algorithm.dataPreprocessing.theEntropyMethod;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**find the maximum and minimum of indexes
 * Created by linwanying on 2016/12/1.
 */
public class PeakReducer extends Reducer<Text, Text, Text, Text> {
    private Logger log = Logger.getLogger("PeakReducer");
    private Text keyOut = new Text();
    private Text valueOut = new Text();
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
            if (fields.length < 2 * action_num) return;
            for (int i = 0; i < action_num; ++i) {
                arrMax[i] = Math.max(arrMax[i], Double.valueOf(fields[i]));
                arrMin[i] = Math.min(arrMin[i], Double.valueOf(fields[i+action_num]));
            }
            log.info(value);
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < action_num; ++i) {
            result.append(arrMax[i]).append("|");
        }
        for (int i = 0; i < action_num; ++i) {
            result.append(arrMin[i]).append("|");
        }
        keyOut.set(key);
        valueOut.set(result.toString());
        log.info("result:"+valueOut);
        context.write(keyOut, valueOut);
    }
}
