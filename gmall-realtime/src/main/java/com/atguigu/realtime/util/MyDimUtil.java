package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.OrderInfo;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/26 10:12
 */
public class MyDimUtil {
    
    public static JSONObject readDimFromPhoenix(Connection conn,
                                                String table,
                                                Object id) throws Exception {
        // select * from table where id=..
        String sql = "select * from " + table + " where id=?";
        List<JSONObject> list = MyJdbcUtil.queryList(conn, sql, new Object[]{id.toString()}, JSONObject.class);
        if (list.size() > 0) {
            return list.get(0);
        }
        return new JSONObject();
    }
    
    public static void main(String[] args) throws Exception {
        /*Connection conn = MyJdbcUtil.getConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
        JSONObject obj = readDim(conn, "dim_user_info", 1);*/
        
        Connection conn = MyJdbcUtil.getConnection("com.mysql.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall2021?user=root&password=aaaaaa");
        //        JSONObject obj = readDim(conn, "user_info", 2);
        List<OrderInfo> obj = MyJdbcUtil.queryList(conn, "select * from order_info", new Object[]{}, OrderInfo.class);
        
        for (OrderInfo hashMap : obj) {
            System.out.println(hashMap);
        }
    }
    
    public static JSONObject readDim(Connection conn,
                                     Jedis client,
                                     String table,
                                     Object id) throws Exception {
        // 先从redis读取维度数据, 如果读到了, 则直接返回对应的维度数据,
        // 没有读到去hbase读取,然后把数据写入到redis缓存
        String key = table + ":" + id;
        String jsonDim = client.get(key);
        if (jsonDim != null) {
            System.out.println(table + " " + id + " 走缓存....");
            // 直接把维度数据返回
            return JSON.parseObject(jsonDim);
        } else {
            System.out.println(table + " " + id + " 走数据库....");
            // 去hbase读取, 然后存入缓存, 还要返回数据
            JSONObject dim = readDimFromPhoenix(conn, table, id);
            // 存入到redis中
            if (!dim.isEmpty()) {
                client.setex(key, 3 * 24 * 60 * 60, dim.toString());
            }
            return dim;
        }
    }
}
