package com.eb.bi.rs.mras.authorrec.itemcf.bookinfo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorBookOnShelfCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    /**
     * @param value: 格式：bookid|authorid|classid|contentstatus|author_grade|typeid
     *               筛选type=1,contentstatus=13的图书
     *               map out:
     *               格式：key:authorid|classid; value:1
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        if (strs.length != 5) {
            return;
        }
        String authorid = strs[1];
        String classid = strs[2];
        String status = strs[3];
        String typeid = strs[4];
        if (!status.equals("13")) {
            return;
        }
        if (!typeid.equals("1")) {
            return;
        }
        String mapKey = String.format("%s|%s", authorid, classid);
        context.write(new Text(mapKey), new IntWritable(1));
    }
}
