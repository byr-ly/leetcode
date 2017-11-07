package com.eb.bi.rs.mras2.booklistrec.sheet_filter_hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liyang on 2016/8/22.
 */
public class SheetFilterHbaseReducer extends Reducer<Text, Text, IntWritable, Text> {

    private int count = 1;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer s = new StringBuffer();
        String bookInfo = "";
        String descInfo = "";
        s.append(key.toString() + "|");
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (line[0].equals("1")) {
                bookInfo = bookInfo + line[2] + "|-1|";
            } else if (line[0].equals("2")) {
                descInfo = descInfo + line[2] + "|" + line[3] + "|" + line[4] + "|" + line[5] + "|";
            }
        }
        if(!bookInfo.equals("") && !descInfo.equals("")){
            s.append(descInfo);
            s.append(bookInfo);
            s.deleteCharAt(s.length() - 1);
            context.write(new IntWritable(count), new Text(s.toString()));
            count++;
        }
    }
}
