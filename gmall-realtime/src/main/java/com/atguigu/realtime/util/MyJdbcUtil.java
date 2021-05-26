package com.atguigu.realtime.util;

import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/26 10:10
 */
public class MyJdbcUtil {
    
    public static Connection getConnection(String driver, String url) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        return DriverManager.getConnection(url);
    }
    
    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args, // 这个用来给sql中的占位符赋值
                                        Class<T> clazz) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        ArrayList<T> result = new ArrayList<>();
    
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            // 进来, 表示有一行数据
            // 读取这行每一列的数据, 封装 T 类型的对象中
            T t = clazz.newInstance();  //利用反射创建一个T类型的对象
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                // 遍历每一列
                // t对象每个属性赋值: 知道属性名和属性值
                String columnName = metaData.getColumnLabel(i + 1);
                Object value = resultSet.getObject(columnName);
    
                BeanUtils.setProperty(t, columnName, value);
            }
            result.add(t);
        }
    
        return result;
    }
    
}
/*
id  name  age
1    zs    20
2    lisi   30


 */
