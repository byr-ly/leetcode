package com.eb.bi.rs.mras.authorrec.itemcf.author_pre_score;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorPreScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 2) {
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }
}
