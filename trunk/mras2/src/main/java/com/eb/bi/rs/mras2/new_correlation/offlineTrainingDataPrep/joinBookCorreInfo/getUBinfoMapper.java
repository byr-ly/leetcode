package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**获取用户图书点击信息
 * 输入格式：是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|分类值|...|免费值|记录时间
 * 输出格式：源图书_操作的图书 B|是否点击|用户|源图书|操作的图书|...|免费值|记录时间
 * Created by linwanying on 2017/6/20.
 */
public class getUBinfoMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 4) return;
        keyOut.set(fields[2]+"_"+fields[3]);
        valueOut.set("B|"+value.toString());
        context.write(keyOut, valueOut);
    }
}

