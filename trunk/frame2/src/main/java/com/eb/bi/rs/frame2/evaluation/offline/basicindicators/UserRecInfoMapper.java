package com.eb.bi.rs.frame2.evaluation.offline.basicindicators;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by houmaozheng on 2016/12/21.
 */
public class UserRecInfoMapper extends Mapper<Object, Text, Text, Text>{

    private String fieldDelimiter;

    protected void setup(Mapper.Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户图书推荐表
        字段：msisdn用户 | bookid图书 | rec_score 推荐图书打分
    输出数据:
        key: msisdn用户 | bookid图书
        value: "rec"
    */
    public void map(Object text, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 3) {
            if (fields[0] != null && fields[1] != null && fields[2] != null) {// 数据不为空
                context.write(new Text(fields[0] + "|" + fields[1]), new Text("rec"));
            }
        }
    }
}
