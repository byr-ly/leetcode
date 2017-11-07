package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight.updateClassWeight;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**读取用户历史
 * Created by linwanying on 2017/6/12.
 */
public class readHistoryMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private static Logger log = Logger.getLogger("updateHbaseDriver");
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 2) return;
        keyOut.set(fields[0]);
        valueOut.set("B|"+fields[1]);
        log.info("history:"+keyOut+"|"+valueOut);
        context.write(keyOut, valueOut);
    }
}
