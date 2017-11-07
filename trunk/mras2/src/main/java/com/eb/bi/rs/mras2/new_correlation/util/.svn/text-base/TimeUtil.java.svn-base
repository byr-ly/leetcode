package com.eb.bi.rs.mras2.new_correlation.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间相关操作
 * Created by linwanying on 2016/7/6.
 */
public class TimeUtil {
    /**
     * 时间串string转为毫秒
     *
     * @param recordTime 14位时间串 格式：yyyy-MM-dd/yyyyMMddHHmmss/yyyyMMdd
     * @return 时间串的毫秒数
     */
    public static Long getTimeMillis(String recordTime, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Date date = null;
        try {
            date = format.parse(recordTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        long time = cal.getTimeInMillis();
        return time;
    }

    /**
     * 将输入时间提前几天
     *
     * @param time 1433494523823L 20150605165523
     * @return 输出为20150605165223
     */
    public static String SeveralDaysAgo(String time, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(getTimeMillis(time, "yyyyMMddHHmmss"));
        calendar.add(Calendar.DAY_OF_MONTH, -days);
        Date date = calendar.getTime();
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return sFormat.format(date);
    }

    /**
     * 毫秒数转为时间串
     *
     * @param recordTime 时间毫秒数
     * @return 14位时间串 格式：yyyyMMdd
     */
    public static String getTimeString(long recordTime) {
        Date date = new Date(recordTime);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date);
    }


    public static String getToday(String pattern) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date.getTime());
    }

    public static void main(String[] args) {
//          System.out.println(getTimeMillis("20160807010000") - getTimeMillis("20160807000000"));
//        System.out.println(getMinToZero("20160806010000"));
//        System.out.println(SeveralDaysAgo("20160807000000", 30));
//        System.out.println(OneDayAgo(1433494523823L));
//        System.out.println(NormalHourAgo(1433494523823L));
//        System.out.println(getToday());
//        System.out.println(getTodayHour());
//        System.out.println(gethour("2016-07-19 06:40:00"));
    }
}
