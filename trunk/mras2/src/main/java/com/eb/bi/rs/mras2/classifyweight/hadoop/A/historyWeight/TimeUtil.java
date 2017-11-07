package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**处理时间
 * Created by linwanying on 2017/3/23.
 */
public class TimeUtil {
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
     * 毫秒数转为时间串
     *
     * @param recordTime 时间毫秒数
     * @return 14位时间串 格式：yyyyMMddHHmmss
     */
    public static String getTimeString(long recordTime) {
        Date date = new Date(recordTime);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(date);
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

    public static String getYesterdayDate() {
        Calendar cal=Calendar.getInstance();
        //System.out.println(Calendar.DATE);//5
        cal.add(Calendar.DATE,-1);
        Date time=cal.getTime();
        return new SimpleDateFormat("yyyyMMdd").format(time);
    }

    public static void main(String[] args) {
        System.out.println(getTimeString(1493319920469L));
        System.out.println(getYesterdayDate());
    }
}
