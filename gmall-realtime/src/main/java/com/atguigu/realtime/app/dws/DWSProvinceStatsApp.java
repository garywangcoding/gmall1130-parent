package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppSQL;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/29 15:15
 */
public class DWSProvinceStatsApp extends BaseAppSQL {
    public static void main(String[] args) {
        new DWSProvinceStatsApp().init(4003, 1, "DWSProvinceStatsApp");
    }
    
    @Override
    protected void run(StreamTableEnvironment tEnv) {
        // 使用sql
        
        // 1. 建立动态表A与source关联(kafka中的topic)
        tEnv.executeSql("create table order_wide(" +
                            "   province_id bigint, " +
                            "   province_name string, " +
                            "   province_area_code string, " +
                            "   province_iso_code string, " +
                            "   province_3166_2_code string, " +
                            "   order_id bigint, " +
                            "   split_total_amount decimal(20, 2), " +
                            "   create_time string, " +
                            "   et as to_timestamp(create_time), " +
                            "   watermark for et as et - interval '5' second " +
                            ")with(" +
                            "   'connector' = 'kafka', " +
                            "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                            "   'properties.group.id' = 'DWSProvinceStatsApp', " +
                            "   'topic' = 'dwm_order_wide', " +
                            "   'format' = 'json', " +
                            "   'scan.startup.mode' = 'earliest-offset' " +
                            ")");
        // 2. 建立动态表B与sink的关联(clickhouse中的table)
        tEnv.sqlQuery("select * from order_wide").execute().print();
        // 3. 在动态表A上执行连续查询
        
        // 4. 连续查询的结果写入到动态表B中
        
    }
}
