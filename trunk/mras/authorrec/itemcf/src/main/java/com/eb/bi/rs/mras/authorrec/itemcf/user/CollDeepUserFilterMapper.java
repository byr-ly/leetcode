package com.eb.bi.rs.mras.authorrec.itemcf.user;

import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CollDeepUserFilterMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    private JobExecuUtil execuUtil = new JobExecuUtil();

    /**
     * @param value: 筛选出推荐库关联作者后的用户-作者打分表
     *               格式：msisdn|authorid|score
     *               map out:
     *               key:msisdn; value:authorid|score
     */

    //TODO 这里的输入输出不对头。
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String group = strs[1];
        if (execuUtil.isBadUser(msisdn)) {
            return;
        }
        int groupid = Integer.parseInt(group);
        if (groupid != 2) {
            return;
        }
        context.write(new Text(msisdn), NullWritable.get());
    }

}
