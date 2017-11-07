package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AuthorFilterReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private int authorCommUserMin = 10;

    /**
     * @param values: 格式：key:authorid, value:score
     *                map out:
     *                key:authorid|score
     */
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {
        int count = 0;
        double sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }
        if (count <= authorCommUserMin) {
            return;
        }
        double avg = sum / count;
        context.write(key, new DoubleWritable(avg));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        authorCommUserMin = context.getConfiguration().getInt(
                PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY, authorCommUserMin);
    }
}
