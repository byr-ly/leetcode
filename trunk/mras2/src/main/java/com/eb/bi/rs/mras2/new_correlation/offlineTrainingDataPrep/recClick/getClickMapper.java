package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**输入用户点击图书
 * 输出格式： 用户 B|推荐类型|推荐图书|时间
 * Created by linwanying on 2017/6/15.
 */
public class getClickMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private Logger log = Logger.getLogger("getClickMapper");
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 4 || !fields[1].equals("3")) return;
        keyOut.set(fields[0]);
        valueOut.set("B|"+value.toString().substring(fields[0].length()+1));
        log.info(keyOut+" "+valueOut);
        context.write(keyOut, valueOut);
    }
}
