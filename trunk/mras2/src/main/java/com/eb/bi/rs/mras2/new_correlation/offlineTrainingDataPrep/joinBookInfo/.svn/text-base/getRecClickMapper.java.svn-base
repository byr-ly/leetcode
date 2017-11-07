package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

/**点击结果输入
 * 输入格式： 是否点击|用户|源图书|推荐图书|类型|时间
 * 输出格式： 推荐图书 B|是否点击|用户|源图书|推荐图书|类型|时间
 * Created by linwanying on 2017/6/19.
 */
public class getRecClickMapper extends Mapper<Object, Text, Text, Text> {
    private Logger log = Logger.getLogger("getRecClickMapper");
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
//        log.info("size:"+fields.length);
        if (fields.length < 6) return;
        keyOut.set(fields[3]);
        valueOut.set("B|"+value.toString());
        context.write(keyOut, valueOut);
    }
}
