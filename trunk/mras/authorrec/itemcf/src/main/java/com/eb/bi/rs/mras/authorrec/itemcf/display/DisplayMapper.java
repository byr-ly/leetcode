package com.eb.bi.rs.mras.authorrec.itemcf.display;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DisplayMapper
        extends Mapper<Object, Text, Text, RecommItemWritable> {
    private Set<String> blacklistSet = new HashSet<String>();

    /**
     * @param value: 已读推荐结果表、预测推荐结果表、补白推荐结果表
     *               格式：msisdn|authorid|bookid|score|type
     *               map out:
     *               格式：key: ; value:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorid = strs[1];
        String bookid = strs[2];
        String temp = String.format("%s|%s", msisdn, bookid);
        if (blacklistSet.contains(temp)) {
            return;
        }
        double score = Double.parseDouble(strs[3]);
        int type = Integer.parseInt(strs[4]);
        RecommendItem item = new RecommendItem(authorid, bookid, score);
        RecommItemWritable writable = new RecommItemWritable(item, type);
        context.write(new Text(msisdn), writable);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String blackPath = context.getConfiguration().get(
                PluginUtil.RECOMM_BLACKLIST_OUT_KEY);
        blackPath = execuUtil.getFileName(blackPath);
        //黑名单. msisdn|bookid|recordday
        for (URI path : localFiles) {
            if (!path.toString().contains(blackPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    String bookid = fields[1];
                    String temp = String.format("%s|%s", msisdn, bookid);
                    blacklistSet.add(temp);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strLog = String.format("set up blacklistSet "
                + "size: %d", blacklistSet.size());
        System.out.println(strLog);
    }
}
