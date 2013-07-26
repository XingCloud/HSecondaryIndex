package com.xingcloud.xa.importtool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: liuxiong
 * Date: 13-7-25
 * Time: 下午5:18
 */
public class TimeUtil {

    public static final String TIMEZONE = "GMT+8";
    public static final TimeZone TZ = TimeZone.getTimeZone(TIMEZONE);

    public static long dayToTimestamp(int day) {
        // 20130701 => timestamp
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

    public static int getDay(long timestamp) {
        // timestamp => 20130701
        final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");
        DF.setTimeZone(TZ);
        Date date = new Date(timestamp);
        return Integer.valueOf(DF.format(date));
    }

    public static int getToday() {
        // return 20130701
        long timestamp = System.currentTimeMillis();
        return getDay(timestamp);
    }

    public static long startTimestampOfToday(){
        int today = getToday();
        return dayToTimestamp(today);
    }

    public static void main(String[] args) {
        System.out.println(getToday());
        System.out.println(startTimestampOfToday());
    }

}
