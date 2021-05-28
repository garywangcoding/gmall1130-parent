package com.atguigu.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/28 10:31
 */
public class MyTimeUtil {
    public static Long dateTimeToTs(String dateTime, String... format) {
        String f = "yyyy-MM-dd HH:mm:ss";  // 默认的时间格式
        if (format.length > 0) {
            f = format[0];
        }
        
        try {
            return new SimpleDateFormat(f).parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
