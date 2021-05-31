package com.atguigu.realtime.util;

import com.atguigu.realtime.bean.VisitorStats;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/31 10:03
 */
public class MySinkUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String clickhouseDb,
                                                        String table,
                                                        Class<T> tClass) {
        
        String url = Constant.CLICKHOSUE_URL_PRE + clickhouseDb;
        String driver = Constant.CLICKHOSUE_DRIVER;
        
        // insert into table(id, name, age) values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
            .append("insert into ")
            .append(table)
            .append("(");
        // 拼接字段的名字: 从类中获取
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            String fn = field.getName();
            sql.append(fn).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        
        sql.append(") values(");
        
        // 拼接问号: 和前面的字段的个数保持一致
        for (Field field : fields) {
            
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return getJdbcSink(url, driver, sql.toString());
        //        return null;
    }
    
    public static void main(String[] args) {
        getClickHouseSink(Constant.CLICKHOUSE_DB, "user", VisitorStats.class);
    }
    
    public static <T> SinkFunction<T> getJdbcSink(String url,
                                                  String driver,
                                                  String sql) {
        
        return JdbcSink.sink(
            sql,
            new JdbcStatementBuilder<T>() {
                
                @Override
                public void accept(PreparedStatement ps,
                                   T t) throws SQLException {
                    // 这里面只需要做一件事情: 把sql中的占位符给设置值
                    // 如果给 ? 设置值?   要根据你的sql的定义
                    try {
                        Field[] fields = t.getClass().getDeclaredFields();
                        for (int i = 0; i < fields.length; i++) {
                            fields[i].setAccessible(true);
                            Object value = fields[i].get(t);
                            ps.setObject(i + 1, value);
                        }
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            },
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                // 添加一些参数
                .withUrl(url)
                .withDriverName(driver)
                .build()
        
        );
    }
    
}
