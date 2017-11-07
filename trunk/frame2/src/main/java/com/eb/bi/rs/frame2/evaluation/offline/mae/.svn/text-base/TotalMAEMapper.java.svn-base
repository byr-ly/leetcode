package com.eb.bi.rs.frame2.evaluation.offline.mae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by houmaozheng on 16/12/5.
 */
public class TotalMAEMapper extends Mapper<Object, Text, Text, Text> {

    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户物品分数交集表
        字段：msisdn用户 | id 物品ID | score 实际打分 | rec_scroe 推荐打分
    输出数据:
        key: "total"所有用户
        value: 分数绝对值 e
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 4) {
            if (fields[0] != null && fields[1] != null && fields[2] != null && fields[3] != null) {// 数据不为空
                float e = Math.abs( Float.parseFloat(fields[2]) - Float.parseFloat(fields[3]));// 分数差绝对值
                context.write(new Text("total"), new Text(String.valueOf(e)));
            }
        }
    }
}
