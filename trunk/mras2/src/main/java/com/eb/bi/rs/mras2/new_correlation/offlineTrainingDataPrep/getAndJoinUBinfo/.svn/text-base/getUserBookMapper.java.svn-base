package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**连接用户和推荐图书交叉信息
 * 输入格式：是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|记录时间
 * 输出格式：用户_操作图书 B|是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|记录时间
 * Created by linwanying on 2017/6/20.
 */
public class getUserBookMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 1) return;
        keyOut.set(fields[1]+"_"+fields[3]);
        valueOut.set("B|"+value.toString());
        context.write(keyOut, valueOut);
    }
}
