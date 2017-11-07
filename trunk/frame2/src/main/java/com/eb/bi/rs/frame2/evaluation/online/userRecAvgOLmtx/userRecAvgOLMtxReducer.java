package com.eb.bi.rs.frame2.evaluation.online.userRecAvgOLmtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 2016/12/21.
 * <p>
 * 输出：msisdn用户 | 推荐位 | 标识 | 用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU | 时间
 */
public class userRecAvgOLMtxReducer extends Reducer<Text, Text, Text, NullWritable> {

    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int Nuc = 0;//Nuc：用户点击的推荐位图书数（去重）
        int Nur = 0;//Nur：用户推荐位展现图书数（去重）
        int Nuo = 0;//Nuo：用户订购的推荐位图书数（去重）

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 2) {
                continue;
            }

            if (fields[0].equals("Nuc")) {
                Nuc = Integer.parseInt(fields[1]);
                continue;
            }
            if (fields[0].equals("Nuo")) {
                Nuo = Integer.parseInt(fields[1]);
                continue;
            }
            if (fields[0].equals("Nur")) {
                Nur = Integer.parseInt(fields[1]);
                continue;
            }
        }

        // 计算用户推荐位图书点击率CTRu = Nuc / Nur
        float CTRu = 0;
        if (Nur != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            CTRu = Float.parseFloat(df.format((float) Nuc / Nur));
        }

        // 计算用户推荐位图书订购率OTRu = Nuo / Nur
        float OTRu = 0;
        if (Nur != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            OTRu = Float.parseFloat(df.format((float) Nuo / Nur));
        }

        // 计算用户推荐位转化率ConU = Nuo / Nuc
        float ConU = 0;
        if (Nuc != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            ConU = Float.parseFloat(df.format((float) Nuo / Nuc));
        }

        context.write(new Text(key.toString() + "|" + CTRu + "|" + OTRu + "|" + ConU + "|" + getNowdate()), NullWritable.get());
    }

    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}
