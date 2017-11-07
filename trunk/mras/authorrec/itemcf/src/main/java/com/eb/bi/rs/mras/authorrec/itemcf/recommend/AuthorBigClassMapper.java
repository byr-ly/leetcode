package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.CounterWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AuthorBigClassMapper
        extends Mapper<Object, Text, Text, CounterWritable> {
    /**
     * @param value: 格式：authorid|classid|count
     *               map out:
     *               key:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[0];
        String classid = strs[1];
        String countstr = strs[2];
        int count = Integer.parseInt(countstr);
        CounterWritable writable = new CounterWritable(classid, count);
        context.write(new Text(authorid), writable);
    }

}
