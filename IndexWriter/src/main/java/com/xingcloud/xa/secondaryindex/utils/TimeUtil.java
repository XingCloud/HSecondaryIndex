package com.xingcloud.xa.secondaryindex.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: IvyTang
 * Date: 12-12-10
 * Time: 下午2:27
 */
public class TimeUtil {

    public static final TimeZone TZ = TimeZone.getTimeZone(Constants.TIMEZONE);

    public static long dayToTimestamp(int day) {
		// this method should only be used by Tail class
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		df.setTimeZone(TZ);
		try{
			Date startDate = df.parse(String.valueOf(day));
			return startDate.getTime();
		} catch (ParseException e){
			e.printStackTrace();
			return -1;
		}
    }

    /**
     * 把时间戳转换为数据库格式日期
     * 格式：yyyyMMddHHmmss
     *
     * @return
     */
    public static long getDate(long timestamp) {
        final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMddHHmmss");
        DF.setTimeZone(TZ);
        Date date = new Date(timestamp);
        return Long.valueOf(DF.format(date));
    }

    /**
     * 把时间戳转换为数据库格式日期
     * 格式：yyyyMMdd
     *
     * @return
     */
    public static int getDay(long timestamp) {
        final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");
        DF.setTimeZone(TZ);
        Date date = new Date(timestamp);
        return Integer.valueOf(DF.format(date));
    }

    public static int getDay(Date date) {
        final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");
        DF.setTimeZone(TZ);
        return Integer.valueOf(DF.format(date));
    }

    public static int nextDay(int yyyy, int mm, int dd) {
        Calendar cal = Calendar.getInstance();
        cal.set(yyyy, mm, dd);
        cal.add(Calendar.DATE, 1);
        return getDay(cal.getTime());
    }

    public static int nextDay(int yyyyMMdd) {
        return nextDay(yyyyMMdd / 10000, yyyyMMdd % 10000 / 100 - 1, yyyyMMdd % 100);
    }

    public static int getToday() {
        //return today: 20130731
        long now = System.currentTimeMillis();
        return getDay(now);
    }
  
    public static void main(String[] args){
        System.out.println(getToday());
        System.out.println(nextDay(20130520));
		System.out.println(dayToTimestamp(20130719));
		System.out.println(dayToTimestamp(20130720));
	}
}
