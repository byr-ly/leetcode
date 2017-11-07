package com.eb.bi.rs.mras.authorrec.itemcf.read;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DeepReadDepthFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Map<String, String> bookAuthorMap = new HashMap<String, String>();
    private Set<String> recommUserAuthors = new HashSet<String>();
    private JobExecuUtil execuUtil = new JobExecuUtil();

    /**
     * @param value: 智能推荐基础数据近6月累计 DMN.IRECM_US_BKID_6CM
     *               格式：msisdn|authorid
     *               过滤:1,msisdn|authoid不在ReadAuthorToRecommFilterMapper筛选后的打分表中
     *               2,只记录深度和超深度阅读记录
     *               map out:
     *               key:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String bookid = strs[1];
        String authorid = bookAuthorMap.get(bookid);
        if (authorid == null) {
            return;
        }
        String ua = String.format("%s|%s", msisdn, authorid);
        if (!recommUserAuthors.contains(ua)) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String bookPath = context.getConfiguration()
                .get(PluginUtil.BOOKINFO_KEY);
        bookPath = execuUtil.getFileName(bookPath);
        String recommScoredPath = context.getConfiguration()
                .get(PluginUtil.TEMP_TORECOMM_USER_READ_KEY);
        recommScoredPath = execuUtil.getFileName(recommScoredPath);
        //格式：bookid|authorid|classid|contentstatus|author_grade|typeid
        for (URI path : localFiles) {
            if (!path.toString().contains(bookPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String bookid = fields[0];
                    String authorid = fields[1];
                    String typeid = fields[5];
                    if (!typeid.equals("1")) {
                        continue;
                    }
                    bookAuthorMap.put(bookid, authorid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        //msisdn|bookid|score
        for (URI path : localFiles) {
            if (path.toString().contains(recommScoredPath)) {
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
                    String authorid = fields[1];
                    String item = String.format("%s|%s", msisdn, authorid);
                    recommUserAuthors.add(item);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("recommUserAuthors size %d",
                recommUserAuthors.size());
        System.out.println(logStr);
    }
}
