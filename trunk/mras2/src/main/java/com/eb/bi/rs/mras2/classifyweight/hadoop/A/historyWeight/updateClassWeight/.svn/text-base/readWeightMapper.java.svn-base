package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight.updateClassWeight;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**读取hbase的用户分类权重表
 * Created by linwanying on 2017/6/12.
 */
public class readWeightMapper extends TableMapper<Text,Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private static Logger log = Logger.getLogger("updateHbaseDriver");

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException, InterruptedException {
        String rowkey = Bytes.toString(key.get());
        String action = Bytes.toString(value.getValue(
                Bytes.toBytes("cf"), Bytes.toBytes("action")));
        String weight = Bytes.toString(value.getValue(
                Bytes.toBytes("cf"), Bytes.toBytes("weight")));
        String latestTime = Bytes.toString(value.getValue(
                Bytes.toBytes("cf"), Bytes.toBytes("latest_time")));
//        log.info("read:"+rowkey);
        if (rowkey == null || action == null || latestTime == null || weight == null) return;
        long todayMillis = getTimeMillis(getTodayDate());
        long latestMillis = getTimeMillis(latestTime);
        double exweight = Double.parseDouble(weight);
        if (latestMillis < todayMillis) {
            if (action.equals("1")) {
                if (todayMillis - latestMillis <= 10 * 24 * 60 * 60 * 1000L) {
                    exweight -= 0.01;
                } else {
                    exweight -= 0.02;
                }

            } else {
                if (todayMillis - latestMillis <= 10 * 24 * 60 * 60 * 1000L) {
                    exweight += 0.01;
                } else {
                    exweight += 0.02;
                }
            }
        }
//        log.info("exweight: " + exweight);
        keyOut.set(rowkey);
        valueOut.set("A|" + action + "|" + new DecimalFormat("0.00").format(exweight) + "|" + latestTime);
        log.info("map-output:"+rowkey + ", " + valueOut);
        context.write(keyOut, valueOut);
    }

    /**
     * 时间串string转为毫秒
     * @param recordTime 12位时间串 格式：yyyyMMdd
     * @return 时间串的毫秒数
     */
    public static Long getTimeMillis(String recordTime) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = format.parse(recordTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        Long time = cal.getTimeInMillis();
        return time;
    }

    /**
     * 获取当日日期，格式：yyyyMMdd
     * @return 当日日期字符串
     */
    public static String getTodayDate() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date.getTime());
    }
}
