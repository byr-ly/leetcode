package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**计算用户所有分类得分的总和
 * Created by linwanying on 2017/3/20.
 */
public class ClassSumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    /**
     * 输入： msisdn|classid|score
     * 输出： key: msisdn  value: score
     */
    private static Logger log = Logger.getLogger("linwanying");
    private Text keyout = new Text();
    private DoubleWritable valueout = new DoubleWritable();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length != 3) {
            return;
        }
        String msisdn = fields[0];
        double score = Double.parseDouble(fields[2]);
        keyout.set(msisdn);
        valueout.set(score);
//        log.info("ClassSumMapper: " + keyout.toString() + "|" + valueout.toString());
        context.write(keyout, valueout);
    }
}
