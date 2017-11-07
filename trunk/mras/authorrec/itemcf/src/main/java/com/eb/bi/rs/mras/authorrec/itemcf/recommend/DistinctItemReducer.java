package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DistinctItemReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Set<String> books = new HashSet<String>();
        for (Text value : values) {
            books.add(value.toString());
        }
        for (String book : books) {
            context.write(key, new Text(book));
        }
    }
}
