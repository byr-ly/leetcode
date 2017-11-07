package com.eb.bi.rs.mras.andnewsrec.get_idf_value;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by liyang on 2016/5/31.
 */
public class GetIdfValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split("\u0001", -1);
        if (fields.length < 4) {
            System.out.println("LoadNewsMapper: Bad Line: " + value.toString());
            return;
        }

        String newsTitle = fields[2].replaceAll("[\\pP￥$^+~|<>\r\n]", " ");
        String newsContent = fields[3].replaceAll("[\\pP￥$^+~|<>\r\n]", " ");

        ArrayList<String> list = SegWord.segWord("N", newsTitle, newsContent);
        for(int i = 0; i < list.size(); i++){
            context.write(new Text(list.get(i)),new IntWritable(1));
        }
    }
}
