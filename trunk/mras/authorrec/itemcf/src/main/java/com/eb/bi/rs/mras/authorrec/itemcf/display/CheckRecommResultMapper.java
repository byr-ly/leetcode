package com.eb.bi.rs.mras.authorrec.itemcf.display;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorInfo;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.BookInfo;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CheckRecommResultMapper extends Mapper<Object, Text, Text, Text> {
    private Map<String, BookInfo> bookMap = new HashMap<String, BookInfo>();

    /**
     * @param value: 用户推荐作者展示表
     *               格式：msisdn|authorid|bookid|。。。
     *               只筛选推荐库中图书对应的作者们
     *               map out:
     *               key:authorid|classid|num
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String bookid = strs[2];
        String authorid = strs[1];
        String type = strs[3];
        String pos = strs[4];
        BookInfo bookInfo = bookMap.get(bookid);
        if (bookInfo == null) {
            String err = String.format("no book %s", bookid);
            System.out.println(err);
        }
        String valout = String.format("%s|%s|%s|%s", authorid,
                bookInfo.toString(), type, pos);
        context.write(new Text(msisdn), new Text(valout));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //格式：bookid|bookname|authorid|classid|contentstatus|class_name|bu_type|auth_name|auth_penname|author_grade
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 10) {
                        continue;
                    }
                    String bookid = fields[0];
                    String bookname = fields[1];
                    String authorid = fields[2];
                    String status = fields[4];
                    String classname = fields[5];
                    String butype = fields[6];
                    String authorname = fields[7];
                    String penname = fields[8];
                    String authorgrade = fields[9];
                    AuthorInfo authorInfo = new AuthorInfo(authorid,
                            authorname, penname, authorgrade);
                    BookInfo bookInfo = new BookInfo(bookname, classname,
                            status, butype);
                    bookInfo.setAuthor(authorInfo);
                    bookMap.put(bookid, bookInfo);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }

    }
}
