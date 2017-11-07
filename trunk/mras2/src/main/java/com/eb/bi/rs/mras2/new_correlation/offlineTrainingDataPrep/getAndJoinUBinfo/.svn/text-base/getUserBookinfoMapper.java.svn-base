package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**获取用户和图书交叉信息
 * 输入格式：用户id|图书id|分类值|新书值|性别值|雅俗值|连载值|免费值
 * 输出格式：用户_图书ID A|分类值|新书值|性别值|雅俗值|连载值|免费值
 * Created by linwanying on 2017/6/20.
 */
public class getUserBookinfoMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 8) return;
        keyOut.set(fields[0]+"_"+fields[1]);
        valueOut.set("A|"+value.toString().substring(fields[0].length()+fields[1].length()+2));
        context.write(keyOut, valueOut);
    }
}
