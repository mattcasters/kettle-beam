package org.kettle.beam.core.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTime {

    public static String toString(Calendar calendar, String pattern){
        if(calendar == null || Strings.isNullOrEmpty(pattern)){return null;}
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(calendar.getTime());
    }

    public static Calendar toCalendar(String value, String pattern) {
        Calendar calendar = null;
        try {
            if (Strings.isNullOrEmpty(value) || Strings.isNullOrEmpty(pattern)) {return null;}
            SimpleDateFormat formatter = new SimpleDateFormat(pattern);
            Date date = formatter.parse(value);
            calendar = Calendar.getInstance();
            calendar.setTime(date);
        }catch (Exception ex){}
        return calendar;
    }

}