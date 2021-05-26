package com.atguigu.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/26 16:08
 */
public class MyThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool() {
        return new ThreadPoolExecutor(
            100,
            300,
            300,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(100));
    }
}
