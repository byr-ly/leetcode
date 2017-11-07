package com.eb.bi.rs.frame2.evaluation.offline.mae;

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
 * 输入：
 *  count | eSum 绝对误差之和
 * 输出：
 *  mae | 时间
 */
public class TotalMAEReducer extends Reducer<Text, Text, Text, NullWritable> {

    private String fieldDelimiter;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        double eSum = 0;
        for (Text pair : values) {
            if (key.toString().equals("total")) {// 所有用户

                String[] fields = pair.toString().split(fieldDelimiter);

                if (fields.length < 2) {
                    continue;
                }

                count += Long.parseLong(fields[0]);
                eSum += Float.parseFloat(fields[1]);
            }
        }

        // 计算MEA = 图书评分绝对值误差总和 / 用户总数
        float mae = 0;
        if ( count != 0) {// 推荐图书数不为0
            DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
            mae = Float.parseFloat(df.format( (float) eSum / count ) );
        }

        String output = mae + "|" + getNowdate();
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
