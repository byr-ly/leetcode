package com.eb.bi.rs.mras.authorrec.itemcf.user;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IncUserScoreFilterMapper
        extends Mapper<Object, Text, Text, Text> {
    private Set<String> userSet = new HashSet<String>();
    private JobExecuUtil execuUtil = new JobExecuUtil();

    /**
     * @param value: 用户-作者打分表
     *               格式：msisdn|authorid|score
     *               map out:
     *               key:authorid; value:msisdn|score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorid = strs[1];
        String scorestr = strs[2];
        if (userSet.contains(msisdn)) {
            return;
        }
        String outval = String.format("%s|%s", authorid, scorestr);
        context.write(new Text(msisdn), new Text(outval));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        //格式：authorid|classid|num
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String user = fields[0];
                    userSet.add(user);
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
