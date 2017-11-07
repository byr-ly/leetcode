package com.eb.bi.rs.frame2.evaluation.offline.basicindicators;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by houmaozheng on 16/12/5.
 */
public class UserRetainInfoMapper extends Mapper<Object, Text, Text, Text> {

    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户图书保留表
        字段：msisdn用户 | bookid图书 | score 图书打分
    输出数据:
        key: msisdn用户 | bookid图书
        value: "retain"
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 3) {
            if (fields[0] != null && fields[1] != null && fields[2] != null) {// 数据不为空
                context.write(new Text(fields[0] + "|" + fields[1]), new Text("retain"));
            }
        }
    }
}
