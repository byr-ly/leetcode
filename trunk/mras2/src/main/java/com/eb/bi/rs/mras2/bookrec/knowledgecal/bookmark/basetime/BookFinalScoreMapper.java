package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark.basetime;

/**
 * Created by liyang on 2016/7/1.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BookFinalScoreMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String[] fields = value.toString().split("\\|");

        if(fields.length >= 6){
            //用户|图书|主要分|次要分|总分|newScore
            context.write(new Text(fields[0]+"|"+fields[1]),new Text("0|"+value.toString()));
        }
        else{
            //用户|图书|最后一次访问时间YYYYMMDD
            context.write(new Text(fields[0]+"|"+fields[1]),new Text("1|"+fields[2]));
        }
    }
}
