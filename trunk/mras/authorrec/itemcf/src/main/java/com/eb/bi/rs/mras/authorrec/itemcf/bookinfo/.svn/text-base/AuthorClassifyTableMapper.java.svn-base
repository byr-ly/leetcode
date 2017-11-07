package com.eb.bi.rs.mras.authorrec.itemcf.bookinfo;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;


public class AuthorClassifyTableMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    private Set<String> authorSet = new HashSet<String>();

    /**
     * 作家分类表
     *
     * @param value: 作者-分类-图书本数表
     *               格式：authorid|classid|num
     *               只筛选推荐库中图书对应的作者们
     *               map out:
     *               key:authorid|classid|num
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[0];
        if (!authorSet.contains(authorid)) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        Set<String> recommBookSet = new HashSet<String>();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String recommBooksPath = context.getConfiguration().get(PluginUtil.RECOMM_BOOK_KEY);
        recommBooksPath = execuUtil.getFileName(recommBooksPath);
        System.out.println(recommBooksPath);
        //推荐库表，只对其中图书匹配的作者进行协同过滤计算
        //格式：bookid
        for (URI path : localFiles) {
            if (!path.toString().contains(recommBooksPath)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length == 0) {
                        continue;
                    }
                    String bookid = fields[0];
                    recommBookSet.add(bookid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strlog = String.format("InitAuthorFilterMapper(recomm " + "books) size: %d", recommBookSet.size());
        System.out.println(strlog);
        String bookPath = context.getConfiguration().get(PluginUtil.BOOKINFO_KEY);
        bookPath = execuUtil.getFileName(bookPath);
        System.out.println(bookPath);
        //格式：bookid|authorid|classid|contentstatus|author_grade|typeid
        for (URI path : localFiles) {
            if (!path.toString().contains(bookPath)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 4) {
                        continue;
                    }
                    String bookid = fields[0];
                    String authorid = fields[1];
                    String typeid = fields[4];
                    if (!typeid.equals("1")) {
                        continue;
                    }
                    if (recommBookSet.contains(bookid)) {
                        authorSet.add(authorid);
                    }
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        strlog = String.format("InitAuthorFilterMapper(author " + "classify table) author size: %d", authorSet.size());
        System.out.println(strlog);
    }
}
