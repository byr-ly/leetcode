package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**过滤超过6个月的数据
 * Created by linwanying on 2017/3/20.
 */
public class UserClassMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    /**
     * 输入： msisdn|bookid|adjust_score|last_read_day
     * 图书信息表：bookid|classid
     * 输出： key: msisdn|class  value: score
     */
    public HashMap<String, String> bookInfo = new HashMap<String, String>();
    public long filter_time = 0L;
    private static Logger log = Logger.getLogger("linwanying");
    private Text keyout = new Text();
    private DoubleWritable valueout = new DoubleWritable();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length != 7 || fields[6].trim().isEmpty() ||
                TimeUtil.getTimeMillis(TimeUtil.getTodayDate()) - TimeUtil.getTimeMillis(fields[6]) >= filter_time) {
            return;
        }

        String msisdn = fields[0];
        String bookid = fields[1];
        String classid;
        Double adjust_score = Double.valueOf(fields[4]);
        if (bookInfo.containsKey(bookid)) {
            classid = bookInfo.get(bookid);
        } else {
            log.info("Can't acquire the classid of book: " + bookid);
            return;
        }
        keyout.set(msisdn + "|" + classid);
        valueout.set(adjust_score);
//        log.info("UserClassMapper-input: " + msisdn + "|" + classid);
        context.write(keyout, valueout);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        filter_time = Long.parseLong(context.getConfiguration().get("filter_time"));
        Configuration conf = context.getConfiguration();
        URI[]  localFiles = context.getCacheFiles();
        if(localFiles == null){
            log.info("图书分类信息表读取失败。");
            return;
        }
        for (URI path : localFiles) {
            String line;
            BufferedReader in = null;
            try {
                FileSystem fs = FileSystem.get(path, conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");
                    if (fields.length < 4) continue;
//                    log.info("bookid: " + fields[0] + " , classid: " + fields[2]);
                    bookInfo.put(fields[0], fields[2]);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}
