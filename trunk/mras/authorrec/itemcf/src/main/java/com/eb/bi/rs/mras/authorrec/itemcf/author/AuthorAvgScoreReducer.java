package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AuthorAvgScoreReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private int authorCommUserMin = 10;

    /**
     * 格式：author|score：计算作者平均分
     * reduce out:
     */
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int num = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            num++;
        }
        if (num < authorCommUserMin) {
            return;
        }
        double avg = sum / num;
        context.write(key, new DoubleWritable(avg));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        authorCommUserMin = context.getConfiguration().getInt(
                PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY, authorCommUserMin);
    }
}
