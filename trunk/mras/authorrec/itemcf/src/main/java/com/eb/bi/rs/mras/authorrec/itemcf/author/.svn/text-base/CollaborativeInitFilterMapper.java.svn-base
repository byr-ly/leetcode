package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CollaborativeInitFilterMapper
        extends Mapper<Object, Text, Text, Text> {
    private Map<String, Integer> authorMap = new HashMap<String, Integer>();
    JobExecuUtil execuUtil = new JobExecuUtil();

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
        Integer idx = authorMap.get(authorid);
        if (idx == null) {
            return;
        }
        String outval = String.format("%d|%s", idx, scorestr);
        context.write(new Text(msisdn), new Text(outval));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        //格式：idx|authorid|avg
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    int idx = Integer.parseInt(fields[0]);
                    String authorid = fields[1];
                    authorMap.put(authorid, idx);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strlog = String.format("CollaborativeInitFilterMapper filter "
                + "author size: %d", authorMap.size());
        System.out.println(strlog);
    }
}
