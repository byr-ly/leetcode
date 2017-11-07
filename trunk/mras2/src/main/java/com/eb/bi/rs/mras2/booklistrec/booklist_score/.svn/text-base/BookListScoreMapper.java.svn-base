package com.eb.bi.rs.mras2.booklistrec.booklist_score;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/4/26.
 */
public class BookListScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 14) {
            String user = line[0];
            StringBuffer trace = new StringBuffer();
            for(int i = 0; i < 14; i++){
                trace.append(line[i] + "|");
            }
            trace.deleteCharAt(trace.length() - 1);
            context.write(new Text(user), new Text(value));
        }
    }
}
