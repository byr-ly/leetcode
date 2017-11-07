package com.eb.bi.rs.mras2.searchtool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by liyang on 2016/12/26.
 * 将要查找用户的离线结果以及实时计算日志提取出来
 * 输入：离线计算结果  字段：用户|...
 *      实时计算结果  字段：...用户...
 */
public class SearchToolMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String userList = conf.get("user_list");
        String date = conf.get("search_date");

        //获取输入文件的路径
        String inputFile = ((FileSplit)context.getInputSplit()).getPath().toString();
        if(inputFile != null && inputFile.matches(".*" + date + ".*")){
            String[] users = userList.split("\\|");
            for(int i = 0; i < users.length; i++){
                String line = value.toString();
                if(line.contains(users[i])){
                    context.write(new Text(users[i]), new Text(line));
                }
            }
        }
    }
}
