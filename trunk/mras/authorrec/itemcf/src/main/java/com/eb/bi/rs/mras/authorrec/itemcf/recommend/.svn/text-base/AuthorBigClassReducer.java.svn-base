package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.CountWritableComparator;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.CounterWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class AuthorBigClassReducer
        extends Reducer<Text, CounterWritable, Text, Text> {

    public void reduce(Text key, Iterable<CounterWritable> values, Context context)
            throws IOException, InterruptedException {
        int saveSize = 5;
        StringBuffer buffer = new StringBuffer();
        List<CounterWritable> writables = new ArrayList<CounterWritable>();
        for (CounterWritable value : values) {
            writables.add(value.clone());
        }
        CountWritableComparator comparator = new CountWritableComparator();
        Collections.sort(writables, comparator);
        for (int i = 0; i < saveSize && i < writables.size(); i++) {
            if (i > 0) {
                buffer.append("|");
            }
            CounterWritable value = writables.get(i);
            buffer.append(value.getId());
        }
        context.write(key, new Text(buffer.toString()));
    }
}
