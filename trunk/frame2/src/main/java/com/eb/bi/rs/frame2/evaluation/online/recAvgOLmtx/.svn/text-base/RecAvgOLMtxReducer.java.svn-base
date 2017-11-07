package com.eb.bi.rs.frame2.evaluation.online.recAvgOLmtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 16/12/5.
 * <p>
 * 输出：
 * 用户ID | 推荐位 | 标识 | 推荐位平均点击率 CTR | 推荐位平均订购率 OTR | 推荐位平均转化率 Con | 时间
 */
public class RecAvgOLMtxReducer extends Reducer<Text, Text, Text, NullWritable> {

    private String fieldDelimiter;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        //用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU
        double CTRuSum = 0;
        double OTRuSum = 0;
        double ConUSum = 0;

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 3) {
                continue;
            }

            count += Long.parseLong(fields[0]);// 用户数
            CTRuSum += Float.parseFloat(fields[1]);// 用户推荐位图书点击率CTRu总和
            OTRuSum += Float.parseFloat(fields[2]);// 用户推荐位图书订购率OTRu总和
            ConUSum += Float.parseFloat(fields[3]);// 用户推荐位转化率ConU总和
        }

        float avgCTR = 0;// 计算推荐位平均点击率 CTR = 用户推荐位图书点击率CTRu总和 / 用户总数
        float avgOTR = 0;// 计算推荐位平均订购率 OTR = 用户推荐位图书订购率OTRu总和 / 用户总数
        float avgCon = 0;// 计算推荐位平均转化率 Con = 用户推荐位转化率ConU总和 / 用户总数

        if (count != 0) {// 推荐图书数不为0

            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            avgCTR = Float.parseFloat(df.format((float) CTRuSum / count));

            df = new DecimalFormat("0.00000");//格式化小数
            avgOTR = Float.parseFloat(df.format((float) OTRuSum / count));

            df = new DecimalFormat("0.00000");//格式化小数
            avgCon = Float.parseFloat(df.format((float) ConUSum / count));

        }

        String output = key.toString() + "|" + avgCTR + "|" + avgOTR + "|" + avgCon + "|" + getNowdate();
        context.write(new Text(output), NullWritable.get());
    }

    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}
