package com.jianbing.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;

public class DateUtils {
    /**
     * @function 获取现在的时间
     * @return
     */
    public static String nowDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new Date());
        return date;
    }

    /**
     * @function 获取过去的时间
     * @param n
     * @return
     */
    public static String lastDate(int n){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE, -n);
        String format = sdf.format(instance.getTime());
        return format;
    }

    /**
     * @param dateType    yyyy-MM-dd,HH:mm:ss, yyyy-MM-dd HH:mm:ss EEEE
     * @param currentTime
     * @return
     * @func input Date--> form Date
     */
    public static String formDateToString(String dateType, Date currentTime) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateType);
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * @param dateType    yyyy-MM-dd,HH:mm:ss, yyyy-MM-dd HH:mm:ss EEEE
     * @param currentTime
     * @return
     */
    public static Date formStringToDate(String dateType, String currentTime) {
        Date date = null;
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(dateType);
            date = formatter.parse(currentTime);
            return date;
        } catch (ParseException e) {

        }
        return date;
    }

    /**
     * @param currentTime
     * @return
     * @function 时间转换成秒 1970/01/01至今的豪秒数（Date转long）
     */
    public static Long formStringToLong(Date currentTime) {
        long stamp = 0L;
        Date date1 = currentTime;
        Date date2 = null;
        try {
            date2 = (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")).parse("1970/01/01 08:00:00");
            stamp = (date1.getTime() - date2.getTime());
        } catch (Exception e) {
            stamp = 0L;
        }
        return stamp;
    }

    /**
     * @param currentTime
     * @return form yyyy-MM-dd HH:mm:ss
     * @func Long-->Date
     */
    public static Date formLongToDate(Long currentTime) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date(currentTime);
            String dateString = formatter.format(date);
            date = formatter.parse(dateString);
            return date;
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * @param dateType    DefaultValue = "yyyy-MM-dd HH:mm:ss" so =""
     * @param currentTime
     * @return
     * @func 将时间由 (毫秒) 转换成指定格式，如(long转：yyyy-MM-dd HH:mm:ss)
     */
    public static String formLongToSting(String dateType, Long currentTime) {
        if (currentTime == null || currentTime == 0) {
            return "";
        }
        if (dateType == null || dateType == "") {
            dateType = "yyyy-MM-dd HH:mm:ss";
        }
        Date da = null;
        try {
            da = (new SimpleDateFormat(dateType)).parse("1970-01-01 08:00:00");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Date date = new Date(da.getTime() + currentTime);
        return (new SimpleDateFormat(dateType)).format(date);
    }

    /**
     * @param date
     * @return
     * @func 判断日期是否有效, 包括闰年的情况
     */
    public static boolean isDate(String date) {
        StringBuffer reg = new StringBuffer("^((\\d{2}(([02468][048])|([13579][26]))-?((((0?");
        reg.append("[13578])|(1[02]))-?((0?[1-9])|([1-2][0-9])|(3[01])))");
        reg.append("|(((0?[469])|(11))-?((0?[1-9])|([1-2][0-9])|(30)))|");
        reg.append("(0?2-?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][12");
        reg.append("35679])|([13579][01345789]))-?((((0?[13578])|(1[02]))");
        reg.append("-?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))");
        reg.append("-?((0?[1-9])|([1-2][0-9])|(30)))|(0?2-?((0?[");
        reg.append("1-9])|(1[0-9])|(2[0-8]))))))");
        Pattern p = Pattern.compile(reg.toString());
        return p.matcher(date).matches();
    }


}
