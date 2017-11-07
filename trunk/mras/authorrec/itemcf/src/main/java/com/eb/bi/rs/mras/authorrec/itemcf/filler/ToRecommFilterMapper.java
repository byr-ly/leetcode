package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class ToRecommFilterMapper
        extends Mapper<Object, Text, Text, NullWritable> {

    private Set<String> initUsers = new HashSet<String>();

    /**
     * @param value: 格式：msisdn|authorid|score
     *               过滤用户不在指定待推荐用户库中的数据
     *               map out:
     *               key:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        if (!initUsers.contains(msisdn)) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    initUsers.add(msisdn);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
}
