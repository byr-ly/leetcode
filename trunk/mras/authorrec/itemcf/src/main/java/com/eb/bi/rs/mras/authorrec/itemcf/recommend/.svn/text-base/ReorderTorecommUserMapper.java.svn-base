package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReorderTorecommUserMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    /**
     * @param value:
     * 格式：authorid|classid|count
     * map out:
     * key:
     */
    private JobExecuUtil execuUtil = new JobExecuUtil();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        if (execuUtil.isBadUser(msisdn)) {
            return;
        }
        context.write(new Text(msisdn), NullWritable.get());
    }

}
