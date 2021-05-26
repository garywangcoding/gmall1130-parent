package com.atguigu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/26 14:06
 */
public class MyRedisUtil {
    private static JedisPool pool;
    
    private MyRedisUtil() {}
    
    public static Jedis getRedisClient() {
        if (pool == null) {
            synchronized (MyRedisUtil.class) {
                if (pool == null) {
                    JedisPoolConfig conf = new JedisPoolConfig();
                    conf.setMaxTotal(1000);
                    conf.setMaxIdle(100);
                    conf.setMinIdle(10);
                    conf.setTestOnBorrow(true);
                    conf.setTestOnReturn(true);
                    conf.setMaxWaitMillis(1000 * 60);
                    pool = new JedisPool(conf, "hadoop162", 6379);
                }
            }
        }
        return pool.getResource();
        
    }
}
