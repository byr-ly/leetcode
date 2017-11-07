package com.eb.bi.rs.mras.bookrec.correrecrealtimefilter;

import java.util.Calendar;

/**
 * @author ynn 
 * @date 创建时间：2015-11-3 上午10:33:33
 * @version 1.0
 */
public class TimeUtil {

	public static long getMillisFromNowToTwelveOclock(int hour) {
		long curTime = System.currentTimeMillis();//;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, hour); // 控制时
		calendar.set(Calendar.MINUTE, 0); // 控制分
		calendar.set(Calendar.SECOND, 0); // 控制秒
		calendar.add(Calendar.DAY_OF_YEAR, +1);  
		return calendar.getTimeInMillis() - curTime;
	}
}
