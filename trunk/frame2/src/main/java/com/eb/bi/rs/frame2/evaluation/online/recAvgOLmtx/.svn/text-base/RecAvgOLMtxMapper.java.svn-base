package com.eb.bi.rs.frame2.evaluation.online.recAvgOLmtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by houmaozheng on 16/12/5.
 */
public class RecAvgOLMtxMapper extends Mapper<Object, Text, Text, Text> {

    private String fieldDelimiter;

    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    /*
    输入数据:
        用户推荐位在线指标信息表表
        字段：msisdn用户 | 推荐位ID | 标识 | 用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU | 时间
    输出数据:
        key: 推荐位ID | 标识 
        value: 用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);

        if (fields.length == 7) {
            if (fields[0] != null && fields[1] != null &&
                    fields[2] != null && fields[3] != null &&
                    fields[4] != null && fields[5] != null) {// 数据不为空

                String key = fields[1] + "|" +fields[2];// 推荐位ID | 标识

                // 用户推荐位图书点击率CTRu | 用户推荐位图书订购率OTRu | 用户推荐位转化率ConU
                String values = fields[3] + "|" + fields[4] + "|" + fields[5];

                context.write(new Text(key), new Text(values));
            }
        }
    }
}
