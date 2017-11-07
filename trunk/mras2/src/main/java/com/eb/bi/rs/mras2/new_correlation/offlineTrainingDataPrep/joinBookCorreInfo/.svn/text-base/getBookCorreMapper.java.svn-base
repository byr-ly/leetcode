package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**获取图书关联度信息
 * 输入格式：源图书A|推荐图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
 * 输出格式：源图书A_推荐图书B A|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
 * Created by linwanying on 2017/6/20.
 */
public class getBookCorreMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 10) return;
        keyOut.set(fields[0]+"_"+fields[1]);
        valueOut.set("A|"+value.toString().substring(fields[0].length()+fields[1].length()+2));
        context.write(keyOut, valueOut);
    }
}
