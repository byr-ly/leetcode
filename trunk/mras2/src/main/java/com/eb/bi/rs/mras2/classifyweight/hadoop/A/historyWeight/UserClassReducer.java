package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.IOException;

/**计算用户在i分类下的历史权重
 * Created by linwanying on 2017/3/20.
 */
public class UserClassReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    /**
     * 输入： key: msisdn|class   value: adjust_score
     * 输出： msisdn|class|hisScore
     */
    private Text keyout = new Text();
    private NullWritable valueout = NullWritable.get();
    private static Logger log = Logger.getLogger("linwanying");
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double score2 = 0.0, score = 0.0, hisScore;
        for (DoubleWritable value : values) {
            score2 += value.get() * value.get();
            score += value.get();
        }
        if (score != 0) {
            hisScore = score2/score;
            keyout.set(key + "|" + hisScore);
//            log.info("UserClassReducer-output: " + key + "|" + hisScore);
            context.write(keyout, valueout);
        }
    }
}
