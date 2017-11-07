package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**获取除了用户统计信息的其它数据信息
 * 输入数据: 是否点击|用户|源图书|操作的图书|...|免费值|前置信度|KULC|IR|记录时间
 * 输出数据: 用户 B|否点击|用户|源图书|操作的图书|...|免费值|前置信度|KULC|IR|记录时间
 * Created by linwanying on 2017/6/23.
 */
public class getInfoMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 4) return;
        keyOut.set(fields[1]);
        valueOut.set("B|"+value.toString());
        context.write(keyOut, valueOut);
    }
}
