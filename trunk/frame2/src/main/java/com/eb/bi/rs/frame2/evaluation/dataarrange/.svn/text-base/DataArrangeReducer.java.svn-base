package com.eb.bi.rs.frame2.evaluation.dataarrange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by houmaozheng on 2016/12/21.
 *
 * 输出：
 * 用户推荐位图书信息表
 * 字段：用户|推荐位|看到的图书ID | 时间
 *
 */
public class DataArrangeReducer extends Reducer<Text, Text, Text, NullWritable> {

    private String fieldDelimiter;
    private String fieldDelimiter2;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
        fieldDelimiter2 = conf.get("field.delimiter2", ",");
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 2) {
                continue;
            }

            String dataTime = fields[1];
            String[] booklist = fields[0].split(fieldDelimiter2);

            for (int i = 0; i<booklist.length; i++) {
                context.write(new Text(key.toString() + "|" + booklist[i] + "|" + dataTime), NullWritable.get());
            }
        }
    }
}
