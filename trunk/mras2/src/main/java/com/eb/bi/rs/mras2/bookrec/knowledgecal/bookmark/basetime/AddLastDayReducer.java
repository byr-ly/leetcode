package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark.basetime;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liyang on 2016/7/1.
 */
public class AddLastDayReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key,Iterable<Text> values,Context context)
        throws IOException,InterruptedException{
        String info = "";
        String time = "";

        // 数据分离
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");

            if (fields[0].equals("0")) {// 0|用户|图书|主要分|次要分|总分|newScore
                info = value.toString().substring(2);
            }

            if (fields[0].equals("1")) {// 1|最后一次访问时间YYYYMMDD
                time = fields[1];
            }
        }

        info = info + "|" + time;
        context.write(new Text(info),NullWritable.get());
    }
}
