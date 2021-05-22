package com.atguigu.realtime.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 10:13
 */
public class MyCommonUtil {
    public static <T> List<T> iterableToList(Iterable<T> it) {
        List<T> result = new ArrayList<>();
        it.forEach(result::add);
        return result;
    }
}
