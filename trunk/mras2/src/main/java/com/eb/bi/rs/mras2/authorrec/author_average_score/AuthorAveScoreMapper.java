package com.eb.bi.rs.mras2.authorrec.author_average_score;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorAveScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 3) {
            DoubleWritable score = new DoubleWritable(Double.parseDouble(line[2]));
            context.write(new Text(line[1]), score);
        }
    }
}
