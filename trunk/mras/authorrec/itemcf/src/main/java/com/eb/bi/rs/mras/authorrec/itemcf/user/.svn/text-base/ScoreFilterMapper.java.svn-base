package com.eb.bi.rs.mras.authorrec.itemcf.user;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ScoreFilterMapper
        extends Mapper<Object, Text, Text, Text> {
    private Set<String> authorSet = new HashSet<String>();
    private static double tolerable_error = 0.01;
    JobExecuUtil execuUtil = new JobExecuUtil();
    private Set<String> deepUserSet = new HashSet<String>();

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
        double score = Double.parseDouble(scorestr);
        if (score < tolerable_error) {
            return;
        }
        if (!authorSet.contains(authorid)) {
            return;
        }
        if (!deepUserSet.contains(msisdn)) {
            return;
        }
        String outval = String.format("%s|%s", authorid, scorestr);
        context.write(new Text(msisdn), new Text(outval));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String authorPath = context.getConfiguration()
                .get(PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY);
        String deepUserPath = context.getConfiguration()
                .get(PluginUtil.TEMP_DEEP_USER_GROUP_6CM_KEY);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        //格式：authorid|classid|num
        for (URI path : localFiles) {
            if (!path.toString().contains(authorPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    authorSet.add(authorid);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strlog = String.format("filter "
                + "author size: %d", authorSet.size());
        System.out.println(strlog);
        for (URI path : localFiles) {
            if (!path.toString().contains(deepUserPath)) {
                continue;
            }
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
