package com.eb.bi.rs.frame2.evaluation.online.recTotalOLMtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by houmaozheng on 2016/12/23.
 */
public class RecTotalOLMtxCombiner extends Reducer<Text,Text,Text,Text> {

    private String fieldDelimiter;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
    }

    // Reduce Method
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long NucSum = 0;//Nuc：用户点击的推荐位图书数（去重）
        long NurSum = 0;//Nur：用户推荐位展现图书数（去重）
        long NuoSum = 0;//Nuo：用户订购的推荐位图书数（去重）
        
        
        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);

            if (fields.length < 2) {
                continue;
            }

            if (fields[0].equals("Nuc")) {
                NucSum += Long.parseLong(fields[1]);
                continue;
            }
            if (fields[0].equals("Nuo")) {
                NuoSum += Long.parseLong(fields[1]);
                continue;
            }
            if (fields[0].equals("Nur")) {
                NurSum += Long.parseLong(fields[1]);
                continue;
            }
        }

        String output = NucSum + "|" + NuoSum + "|" + NurSum;
        context.write(key, new Text(output));
    }
}
