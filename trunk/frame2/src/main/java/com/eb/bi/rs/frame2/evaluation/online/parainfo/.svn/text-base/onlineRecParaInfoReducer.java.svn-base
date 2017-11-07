package com.eb.bi.rs.frame2.evaluation.online.parainfo;

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
 * 输出：（三者之一）
 * 用户推荐位图书数（去重）表
 * msisdn用户 | 推荐位 | 图书数（去重） | 时间
 *
 * 用户推荐位点击图书数（去重）表
 * msisdn用户 | 推荐位 | 图书数（去重） | 时间
 *
 * 用户推荐位订购图书数（去重）表
 * msisdn用户 | 推荐位 | 图书数（去重） | 时间
 *
 */
public class onlineRecParaInfoReducer extends Reducer<Text, Text, Text, NullWritable> {

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Set bookIDList = new HashSet();

        for (Text pair : values) {
            if (pair.toString() != null) {
                bookIDList.add(pair.toString());
            }
        }

        context.write(new Text(key.toString() + "|" + bookIDList.size() + "|" + getNowdate()), NullWritable.get());
    }

    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}
