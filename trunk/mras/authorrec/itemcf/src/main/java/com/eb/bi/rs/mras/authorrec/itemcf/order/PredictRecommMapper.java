package com.eb.bi.rs.mras.authorrec.itemcf.order;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PredictRecommMapper
        extends Mapper<Object, Text, Text, RecommItemWritable> {

    private Map<String, String> bookInfoMap = new HashMap<String, String>();
    private Map<String, String> authorClassMap = new HashMap<String, String>();

    /**
     * @param value: 输入：预测用户偏好作者（排序后）
     *               格式：msisdn|第n偏好|偏好classid|authorid1|score1|author2|score2|...
     *               map out:
     *               key:msisdn, value:authorid1|bookid1|score1|prefidx|...
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        int prefIdx = Integer.parseInt(strs[1]);
        String classid = strs[2];
        for (int i = 3; i < strs.length - 1; i += 2) {
            String authorid = strs[i];
            String scoreStr = strs[i + 1];
            double score = Double.parseDouble(scoreStr);
            if (prefIdx < 0) {
                classid = authorClassMap.get(authorid);
            }
            String temp = String.format("%s|%s", authorid, classid);
            String bookid = bookInfoMap.get(temp);
            if (bookid == null) {
                continue;
            }
            RecommendItem item = new RecommendItem(authorid, bookid, score);
            RecommItemWritable recommItem = new RecommItemWritable(item, prefIdx);
            context.write(new Text(msisdn), recommItem);
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String bookAuthorPath = context.getConfiguration().get(PluginUtil.BOOKINFO_KEY);
        bookAuthorPath = execuUtil.getFileName(bookAuthorPath);
        String authorClassPath = context.getConfiguration()
                .get(PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY);
        authorClassPath = execuUtil.getFileName(authorClassPath);
        //格式：格式：bookid|authorid|classid|contentstatus|author_grade|typeid
        for (URI path : localFiles) {
            if (!path.toString().contains(bookAuthorPath)) {
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
                    String classid = fields[2];
                    String status = fields[3];
                    String typeid = fields[5];
                    if (!status.equals("13")) {
                        continue;
                    }
                    if (!typeid.equals("1")) {
                        continue;
                    }
                    String key = String.format("%s|%s", authorid, classid);
                    if (bookInfoMap.containsKey(key)) {
                        continue;
                    }
                    bookInfoMap.put(key, bookid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        //作者所写图书本数最多的分类
        //格式：authorid|classid1|classid2|classid3....
        for (URI path : localFiles) {
            if (!path.toString().contains(authorClassPath)) {
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
                    String classid = fields[1];
                    authorClassMap.put(authorid, classid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
}
