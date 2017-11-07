package com.eb.bi.rs.mras.authorrec.itemcf.read;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadRecommendMapper extends Mapper<Object, Text, Text, Text> {
    private Map<String, List<String>> authorBigClasses = new HashMap<String, List<String>>();
    private Map<String, Set<String>> bookInfoMap = new HashMap<String, Set<String>>();
    private Map<String, Set<String>> readDepthMap = new HashMap<String, Set<String>>();

    /**
     * @param value: 筛选后的用户-作者打分表：msisdn|authorid|score
     *               map out:
     *               key:msisdn; key:authorid|bookid|score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorid = strs[1];
        String scoreStr = strs[2];
        List<String> classids = authorBigClasses.get(authorid);
        if (classids == null) {
            return;
        }
        Set<String> readBooks = readDepthMap.get(msisdn);
        for (String classid : classids) {
            String temp = String.format("%s|%s", authorid, classid);
            Set<String> booksList = bookInfoMap.get(temp);
            if (booksList == null) {
                continue;
            }
            for (String bookid : booksList) {
                if (readBooks == null || !readBooks.contains(bookid)) {
                    String mapVal = String.format("%s|%s|%s",
                            authorid, bookid, scoreStr);
                    context.write(new Text(msisdn), new Text(mapVal));
                    return;
                }
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String authorBigClassPath = context.getConfiguration()
                .get(PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY);
        authorBigClassPath = execuUtil.getFileName(authorBigClassPath);
        String bookAuthorPath = context.getConfiguration()
                .get(PluginUtil.BOOKINFO_KEY);
        bookAuthorPath = execuUtil.getFileName(bookAuthorPath);
        String deepReadFilterOut = context.getConfiguration()
                .get(PluginUtil.TEMP_DEEP_READ_FILTER_READ_KEY);
        deepReadFilterOut = execuUtil.getFileName(deepReadFilterOut);
        //authorid|classid1|classid2|...
        for (URI path : localFiles) {
            if (!path.toString().contains(authorBigClassPath)) {
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
                    List<String> classList = new ArrayList<String>();
                    for (int i = 1; i < fields.length; i++) {
                        classList.add(fields[i]);
                    }
                    authorBigClasses.put(authorid, classList);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
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
                    Set<String> books = bookInfoMap.get(key);
                    if (books == null) {
                        books = new HashSet<String>();
                    }
                    books.add(bookid);
                    bookInfoMap.put(key, books);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        //近6个月用户深度阅读记录
        //格式：msisdn|bookid|depth
        for (URI path : localFiles) {
            if (!path.toString().contains(deepReadFilterOut)) {
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
                    Set<String> books = readDepthMap.get(msisdn);
                    if (books == null) {
                        books = new HashSet<String>();
                    }
                    books.add(bookid);
                    readDepthMap.put(msisdn, books);
                    //	String temp = String.format("%s|%s", msisdn, bookid);
                    //	readDepthMap.add(temp);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
}
