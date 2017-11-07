package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**加载用户分类得分
 * Created by linwanying on 2017/4/12.
 */
public class UCScoreMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyout = new Text();
    private Text valueout = new Text();
    private static Logger log = Logger.getLogger("linwanying");
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length != 2) {
            return;
        }

        String msisdn = fields[0];
        String score = fields[1];
        keyout.set(msisdn);
        valueout.set("A|" + score);
//        log.info(keyout.toString() + "|" + valueout.toString());
        context.write(keyout, valueout);
    }
}
