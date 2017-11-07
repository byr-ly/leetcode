package com.eb.bi.rs.frame2.evaluation.offline.basicindicators;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 2016/12/22.
 */
public class OfflineBasisMapper extends Mapper<Object, Text, Text, Text> {

    private String fieldDelimiter;

    protected void setup(Mapper.Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户图书统计表
        字段：推荐用户图书数 | 保留用户图书数 | 保留且推荐用户图书数 | 日期
    输出数据:
        key: null
        value: 准确率 | 召回率 | F1值 | 日期
    */
    public void map(Object text, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 4) {
            float totalAccuracy = 0;// 整体准确率
            float totalRecallRate = 0;// 整体召回率
            float totalF1 = 0;// F1值

            int recCount = Integer.parseInt(fields[0]);
            int retainCount = Integer.parseInt(fields[1]);
            int likeCount = Integer.parseInt(fields[2]);

            // 计算准确率 = 用户喜欢的推荐图书数 / 用户图书总推荐数
            if ( recCount != 0) {// 推荐图书数不为0

                DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
                totalAccuracy = Float.parseFloat(df.format( (float) likeCount / recCount ) );
            }

            // 计算召回率 = 用户喜欢的推荐图书数 / 用户喜欢的图书总数（即保留的图书数）
            if ( retainCount != 0) {// 推荐图书数不为0
                DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
                totalRecallRate = Float.parseFloat(df.format( (float) likeCount / retainCount ) );
            }

            // 计算 F1 值 = 准确率P * 召回率R * 2 / (准确率P + 召回率R)
            if ( totalAccuracy + totalRecallRate != 0 ) {// 推荐图书数不为0
                DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
                totalF1 = Float.parseFloat( df.format( (totalAccuracy * totalRecallRate * 2) / (totalAccuracy + totalRecallRate) ) );//返回的是String类型
            }

            String output = totalAccuracy + "|" + totalRecallRate + "|" + totalF1 + "|" + getNowdate();
            context.write(new Text(output), NullWritable.get());
        }
    }

    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}