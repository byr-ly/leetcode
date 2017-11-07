package com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.util.TextPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;

public class ItemFilterReducer extends Reducer<TextPair, TextPair, NullWritable, Text> {

    private String mode;

    @Override
    protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws java.io.IOException, InterruptedException {

        Iterator<TextPair> iter = values.iterator();
        TextPair pair = iter.next();

        if (pair.getSecond().toString().equals("1") && mode.equals("exclude")) {
            context.write(NullWritable.get(), pair.getFirst());
            while (iter.hasNext()) {
                context.write(NullWritable.get(), iter.next().getFirst());
            }
        } else if (pair.getSecond().toString().equals("0") && mode.equals("include")) {
            while (iter.hasNext()) {
                context.write(NullWritable.get(), iter.next().getFirst());
            }
        }
    }

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        mode = context.getConfiguration().get("filter.mode", "exclude");
    }

}
