package com.eb.bi.rs.mras.authorrec.itemcf.readscore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ReadAuthorScoreMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private Map<String, String> bookAuthorMap = new HashMap<String, String>();
    private float filterScore = 0;
    private JobExecuUtil execuUtil = new JobExecuUtil();
    private Logger log = null;

    /**
     * @param value: 用户图书打分结果表 dmn.irecm_us_bkid_score_all
     *               格式：msisdn|book_id|book_score
     *               过滤打分小于0.01的记录
     *               map out:
     *               key:msisdn|authorid; value:score
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        if (strs.length < 3) {
            return;
        }
        String msisdn = strs[0];
        String bookid = strs[1];
        String scoreStr = strs[2];
        double score = Double.parseDouble(scoreStr);
        if (filterScore > 0 && score < filterScore) {
            return;
        }
        if (execuUtil.isBadUser(msisdn)) {
            return;
        }
        String authorid = bookAuthorMap.get(bookid);
        if (authorid == null) {
            return;
        }
        String mapkey = String.format("%s|%s", msisdn, authorid);
        context.write(new Text(mapkey), new DoubleWritable(score));
    }

    @Override
    public void setup(Context context) throws InterruptedException, IOException {
        log = Logger.getLogger(ReadAuthorScoreMapper.class);
        filterScore = context.getConfiguration().getFloat(PluginUtil.BOOK_SCORE_MIN_KEY, 0);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String bookInfoKey = PluginUtil.BOOKINFO_KEY;
        String bookPath = context.getConfiguration().get(bookInfoKey);
        bookPath = execuUtil.getFileName(bookPath);
        System.out.println(bookPath);
        log.info("ReadAuthorScoreMapper set up begin");
        //格式：bookid|authorid|classid|contentstatus|author_grade|typeid
        //不过滤contentstatus=13的图书
        for (URI path : localFiles) {
            if (!path.toString().contains(bookPath)) {
                continue;
            }
            log.info("Path" + path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String bookid = fields[0];
                    String authorid = fields[1];
                    String typeid = fields[4];
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
        String logStr = String.format("ReadAuthorScoreMapper set up " + "bookAuthorMap size: %s", bookAuthorMap.size());
        log.info(logStr);
    }


    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {
        bookAuthorMap.clear();
        bookAuthorMap = null;
    }

}
