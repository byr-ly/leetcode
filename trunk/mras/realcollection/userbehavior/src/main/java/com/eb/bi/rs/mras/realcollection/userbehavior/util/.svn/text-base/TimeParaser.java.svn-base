package com.eb.bi.rs.mras.realcollection.userbehavior.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by LiMingji on 2015/5/26.
 */
public class TimeParaser {
    private static Logger log =Logger.getLogger(TimeParaser.class);
    /**
     * @param recordTime 输入样例 20150505165523
     * @return 从recordTime到1970年的毫秒数
     */
    public static long splitTime(String recordTime) {
        Calendar calendar = Calendar.getInstance();
        try {
            int year = Integer.parseInt(recordTime.substring(0, 4));
            int month = Integer.parseInt(recordTime.substring(4, 6));
            int date = Integer.parseInt(recordTime.substring(6, 8));
            int hour = Integer.parseInt(recordTime.substring(8, 10));
            int minute = Integer.parseInt(recordTime.substring(10, 12));
            int seconds = Integer.parseInt(recordTime.substring(12, 14));

            calendar.set(year, month - 1, date, hour, minute, seconds);
            return (calendar.getTimeInMillis()/1000)*1000;

        } catch (Exception e) {
            log.error("时间输入格式有问题: " + e);
        }
        return -1L;
    }

    /**
     *根据long型构造符合条件的日期格式
     * @param inputTime  1433494523823L
     * @return 输出为20150621121500
     */
    public static String formatTimeInSeconds(Long inputTime) {
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date(inputTime);
        return sFormat.format(date);
    }

    /**
     * 通过long获得日期。
     * @param time
     * @return
     */
    public static int getDateFromLong(long time) {
        Date date = new Date(time);
        return date.getDay();
    }

    public static void main(String[] args) {
        System.out.println(getDateFromLong(System.currentTimeMillis()));
    }
}
