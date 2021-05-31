package com.atguigu.realtime.app.function;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyJdbcUtil;
import com.atguigu.realtime.util.MyRedisUtil;
import com.atguigu.realtime.util.MyThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/31 16:05
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    private ThreadPoolExecutor pool;
    private Connection conn;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        pool = MyThreadPoolUtil.getThreadPool();
        
        conn = MyJdbcUtil.getConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
        
    }
    
    protected abstract void addDim(Connection conn,
                                   Jedis client,
                                   T input,
                                   ResultFuture<T> resultFuture) throws Exception;
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        pool.execute(new Runnable() {
            @Override
            public void run() {
                // 一个异步操作, 一个redis的客户端
                Jedis client = MyRedisUtil.getRedisClient();
                try {
                    addDim(conn, client, input, resultFuture);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // 把客户端还给连接池
                client.close();
                
            }
        });
    }
    
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        
        if (pool != null) {
            pool.shutdown();
        }
    }
}
