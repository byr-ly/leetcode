package com.eb.bi.rs.frame2.evaluation.online.recTotalOLMtx;

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
 * 推荐位 | 推荐位总体点击率 CTR | 推荐位总体订购率 OTR | 推荐位总体转化率 Con | 时间
 */
public class RecTotalOLMtxReducer extends Reducer<Text, Text, Text, NullWritable> {

    private String fieldDelimiter;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long NucSum = 0;//Nuc：用户点击的推荐位图书数（去重）总数
        long NurSum = 0;//Nur：用户推荐位展现图书数（去重）总数
        long NuoSum = 0;//Nuo：用户订购的推荐位图书数（去重）总数

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 3) {
                continue;
            }

            NucSum += Long.parseLong(fields[0]);
            NuoSum += Long.parseLong(fields[1]);
            NurSum += Long.parseLong(fields[2]);
        }


        // 计算推荐位总体图书点击率CTRu = Nuc / Nur
        float totalCTR = 0;
        if (NurSum != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            totalCTR = Float.parseFloat(df.format((float) NucSum / NurSum));
        }

        // 计算推荐位总体图书订购率OTRu = Nuo / Nur
        float totalOTR = 0;
        if (NurSum != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            totalOTR = Float.parseFloat(df.format((float) NuoSum / NurSum));
        }

        // 计算推荐位总体转化率ConU = Nuo / Nuc
        float totalCon = 0;
        if (NucSum != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            totalCon = Float.parseFloat(df.format((float) NuoSum / NucSum));
        }

        String output = key.toString() + "|" + totalCTR + "|" + totalOTR + "|" + totalCon + "|" + getNowdate();
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
