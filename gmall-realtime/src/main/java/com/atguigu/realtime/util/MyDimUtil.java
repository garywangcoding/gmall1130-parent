package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.OrderInfo;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/26 10:12
 */
public class MyDimUtil {
    
    public static JSONObject readDim(Connection conn,
                                     String table,
                                     Object id) throws Exception {
        // select * from table where id=..
        String sql = "select * from " + table + " where id=?";
        List<JSONObject> list = MyJdbcUtil.queryList(conn, sql, new Object[]{id.toString()}, JSONObject.class);
        if(list.size() > 0){
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
}
