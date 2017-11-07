package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

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

public class ReorderUserReadDepthMapper
        extends Mapper<Object, Text, Text, Text> {
    private JobExecuUtil execuUtil = new JobExecuUtil();
    private Set<String> incUsers = null;

    /**
     * @param value: 格式：智能推荐基础数据（6月累计）
     *               map out:
     *               key:
     */

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String bookid = strs[1];
        int depth = Integer.parseInt(strs[2]);
        if (execuUtil.isBadUser(msisdn)) {
            return;
        }
        if (depth == 0) {
            return;
        }
        if (incUsers != null && !incUsers.contains(msisdn)) {
            return;
        }
        context.write(new Text(msisdn), new Text(bookid));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        if (localFiles == null) {
            return;
        }
        incUsers = new HashSet<String>();
        //格式：idx|author|avg
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    try {
                        String user = fields[0];
                        incUsers.add(user);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }

}
