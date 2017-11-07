package com.eb.bi.rs.mras.unifyrec.realtimerec.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author ynn
 * @date 创建时间：2015-11-3 上午10:33:33
 * @version 1.0
 */
public class TimeUtil {

	public static long getMillisFromNowToTwelveOclock(int hour) {
		long curTime = System.currentTimeMillis();// ;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, hour); // 控制时
		calendar.set(Calendar.MINUTE, 0); // 控制分
		calendar.set(Calendar.SECOND, 0); // 控制秒
		calendar.add(Calendar.DAY_OF_YEAR, +1);
		return calendar.getTimeInMillis() - curTime;
	}

	/*
	 * 默认定时器是12点
	 */
	public static long getMillisFromNowToTwelveOclock() {
		long curTime = System.currentTimeMillis();// ;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 12); // 控制时
		calendar.set(Calendar.MINUTE, 0); // 控制分
		calendar.set(Calendar.SECOND, 0); // 控制秒
		calendar.add(Calendar.DAY_OF_YEAR, +1);
		return calendar.getTimeInMillis() - curTime;
	}

	/*
	 * 比较日期与当时日期
	 * @return 在当前日期前返回1，在当前日期后返回-1，当天返回0
	 */
	public static int compareDate(String expireDate) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date dt1 = df.parse(expireDate);
			Date dt2 = df.parse(df.format(new Date()));
			if (dt1.getTime() <= dt2.getTime()) {
				return -1;
			} else {
				return 1;
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return 0;
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
    
    public static int getDaysInterval(String lastDay) {
        return (int)(System.currentTimeMillis() - getTimeMillis(lastDay))/(1000*24*60*60);
    }
    
	public static void main(String[] args){
		compareDate("2016-07-11");
	}
	
}
