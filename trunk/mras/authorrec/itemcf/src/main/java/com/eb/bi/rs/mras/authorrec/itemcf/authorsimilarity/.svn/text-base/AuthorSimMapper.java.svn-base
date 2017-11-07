package com.eb.bi.rs.mras.authorrec.itemcf.authorsimilarity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorSimMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if(line.length >= 13){
            StringBuffer result = new StringBuffer();
            for(int i = 2; i < line.length; i++){
                result.append(line[i] + "|");
            }
            result.deleteCharAt(result.length() - 1);
            context.write(new Text(line[0] + "|" + line[1]), new Text(result.toString()));
        }
    }
}
