package com.xingcloud.xa.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/27/13
 * Time: 11:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class DateUtil {

    public static String getTodayDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Date date = new Date();
        return sdf.format(date);
    }

    public static String getYesterdayDateStr() {
        TimeZone tz = TimeZone.getTimeZone("GMT+8");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(tz);
        Calendar cal = Calendar.getInstance(tz) ;
        cal.add(Calendar.DATE,-1);
        Date date = cal.getTime();
        return sdf.format(date);
    }

    public static String getTomorrowDateStr(){
        TimeZone tz = TimeZone.getTimeZone("GMT+8");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(tz);
        Calendar cal = Calendar.getInstance(tz);
        cal.add(Calendar.DATE,+1);
        Date date = cal.getTime() ;
        return  sdf.format(date);
    }
}
