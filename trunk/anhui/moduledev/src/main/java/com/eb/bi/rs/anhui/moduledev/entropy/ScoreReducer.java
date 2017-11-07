package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**求用户品牌综合得分
 * Created by linwanying on 2016/11/16.
 */
public class ScoreReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String info = key.toString();
        for (Text value : values) {
            info +=  "|" + value.toString();
        }
        context.write(new Text(info), NullWritable.get());
    }
}
