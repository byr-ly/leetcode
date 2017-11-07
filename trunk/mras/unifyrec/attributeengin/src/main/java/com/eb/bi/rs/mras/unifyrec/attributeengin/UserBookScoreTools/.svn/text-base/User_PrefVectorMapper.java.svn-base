package com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class User_PrefVectorMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        context.write(new Text(fields[0]), new Text("C|" + value.toString()));
    }
}