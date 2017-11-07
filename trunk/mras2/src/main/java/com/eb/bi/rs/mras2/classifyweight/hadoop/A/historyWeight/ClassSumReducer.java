package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**计算用户所有分类的得分总和
 * Created by linwanying on 2017/3/20.
 */
public class ClassSumReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    /**
     * 输入： key: msisdn   value: score
     * 输出： msisdn|scoresum
     */
    private static Logger log = Logger.getLogger("linwanying");
    private Text keyout = new Text();
    private NullWritable valueout = NullWritable.get();
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double scoresum =  0.0;
        for (DoubleWritable value : values) {
            scoresum += value.get();
        }
        keyout.set(key + "|" + String.valueOf(scoresum));
//        log.info("ClassSumReducer: " + keyout.toString() + "|" + valueout.toString());
        context.write(keyout, valueout);
    }
}
