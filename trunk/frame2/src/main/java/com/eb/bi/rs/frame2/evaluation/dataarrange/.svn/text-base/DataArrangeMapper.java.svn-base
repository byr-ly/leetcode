package com.eb.bi.rs.frame2.evaluation.dataarrange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by houmaozheng on 2017/1/3.
 */
public class DataArrangeMapper extends Mapper<Object, Text, Text, Text> {
    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户推荐位图书信息表
        字段：用户|推荐位|看到的图书ID1, 看到的图书ID2, 看到的图书ID3... | 时间

    输出数据:
        key: msisdn用户 | 推荐位
        value: 看到的图书ID1, 看到的图书ID2, 看到的图书ID3... | 时间
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 4) {
            if (fields[0] != null && fields[1] != null &&
                    fields[2] != null && fields[3] != null) {// 数据不为空
                context.write(new Text(fields[0] + "|" + fields[1] ), new Text(fields[2] + "|" + fields[3]));
            }
        }
    }
}
