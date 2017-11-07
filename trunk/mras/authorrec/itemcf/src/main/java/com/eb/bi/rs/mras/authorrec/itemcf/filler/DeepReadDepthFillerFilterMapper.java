package com.eb.bi.rs.mras.authorrec.itemcf.filler;

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


public class DeepReadDepthFillerFilterMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    private Map<String, String> bookAuthorMap = new HashMap<String, String>();
    private Set<String> recommUsers = new HashSet<String>();

    /**
     * @param value: 智能推荐基础数据近6月累计 DMN.IRECM_US_BKID_6CM
     *               格式：msisdn|bookid|read_depth
     *               过滤:1,msisdn为非补白用户
     *               2,只记录深度和超深度阅读记录
     *               3,作者为非推荐库关联用户,或非名家
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
        if (!recommUsers.contains(msisdn)) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String bookPath = context.getConfiguration().get(
                PluginUtil.TEMP_AUTHOR_FAMOUS_BOOK_KEY);
        bookPath = execuUtil.getFileName(bookPath);
        String recommUserPath = context.getConfiguration()
                .get(PluginUtil.TEMP_TORECOMM_USER_FILLER_KEY);
        recommUserPath = execuUtil.getFileName(recommUserPath);
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
                    bookAuthorMap.put(bookid, authorid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("famous author size %d", bookAuthorMap.size());
        System.out.println(logStr);
        //msisdn|偏好|偏好classid
        System.out.println(recommUserPath);
        for (URI path : localFiles) {
            if (!path.toString().contains(recommUserPath)) {
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
                    recommUsers.add(msisdn);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        logStr = String.format("recommUsers size %d", recommUsers.size());
        System.out.println(logStr);
    }
}
