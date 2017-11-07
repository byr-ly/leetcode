package com.eb.bi.rs.mras.authorrec.itemcf.user;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ReorderUserMapper
        extends Mapper<Object, Text, Text, Text> {
    /**
     * @param value: map out:
     *               key:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String right = value.toString().substring(msisdn.length() + 1);
        context.write(new Text(msisdn), new Text(right));
    }
}
