package com.eb.bi.rs.mras.generec.storm.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author ynn
 * @version 1.0
 * @date 创建时间：2015-11-3 上午10:33:33
 */
public class TimeUtil {
    public static void main(String[] args) {
        System.out.println(getMillisFromNowToTwelveOclock(12));
    }

    public static long getMillisFromNowToTwelveOclock(int hour) {
        long curTime = System.currentTimeMillis();//;
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, hour); // 控制时
        calendar.set(Calendar.MINUTE, 0); // 控制分
        calendar.set(Calendar.SECOND, 0); // 控制秒
        calendar.add(Calendar.DAY_OF_YEAR, +1);
        return calendar.getTimeInMillis() - curTime;
    }

    public static long getMillisFromNowToTwelveOclock(int hour, int min) {
        long curTime = System.currentTimeMillis();//;
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, hour); // 控制时
        calendar.set(Calendar.MINUTE, min); // 控制分
        calendar.set(Calendar.SECOND, 0); // 控制秒
        calendar.add(Calendar.DAY_OF_YEAR, +1);
        return calendar.getTimeInMillis() - curTime;
    }

    /*
     * 默认定时器是12点
     */
    public static long getMillisFromNowToTwelveOclock() {
        long curTime = System.currentTimeMillis();//;
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 12); // 控制时
        calendar.set(Calendar.MINUTE, 0); // 控制分
        calendar.set(Calendar.SECOND, 0); // 控制秒
        calendar.add(Calendar.DAY_OF_YEAR, +1);
        return calendar.getTimeInMillis() - curTime;
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

    /**
     * 获取当日日期，格式：yyyyMMdd
     * @return 当日日期字符串
     */
    public static String getTodayDate1() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date.getTime());
    }

    public static int getDaysInterval(String lastDay) {
        return (int)(System.currentTimeMillis() - getTimeMillis(lastDay))/(1000*24*60*60);
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

}

