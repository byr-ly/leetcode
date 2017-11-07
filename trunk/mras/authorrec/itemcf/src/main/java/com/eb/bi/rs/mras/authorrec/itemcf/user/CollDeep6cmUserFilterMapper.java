package com.eb.bi.rs.mras.authorrec.itemcf.user;

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


public class CollDeep6cmUserFilterMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    JobExecuUtil execuUtil = new JobExecuUtil();
    private Set<String> deepUserSet = new HashSet<String>();

    /**
     * @param value: 6cm用户表
     *               map out:
     *               key:msisdn
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        if (!deepUserSet.contains(msisdn)) {
            return;
        }
        context.write(new Text(msisdn), NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String user = fields[0];
                    deepUserSet.add(user);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
}
