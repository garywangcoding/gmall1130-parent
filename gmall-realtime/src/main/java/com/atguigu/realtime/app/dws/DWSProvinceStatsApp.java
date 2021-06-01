package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppSQL;
import org.apache.flink.table.api.Table;
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
                            "   'scan.startup.mode' = 'latest-offset' " +
                            ")");
//        tEnv.sqlQuery("select * from order_wide").execute().print();
        // 2. 建立动态表B与sink的关联(clickhouse中的table)
        tEnv.executeSql("create table province_stats_2021(" +
                            "   stt string, " +
                            "   edt string, " +
                            "   province_id bigint, " +
                            "   province_name string, " +
                            "   area_code string, " +
                            "   iso_code string, " +
                            "   iso_3166_2 string, " +
                            "   order_amount decimal(20, 2), " +
                            "   order_count bigint, " +
                            "   ts bigint, " +
                            "   primary key(stt, edt, province_id) not enforced " +
                            ")with(" +
                            "   'connector' = 'clickhouse', " +
                            "   'url' = 'clickhouse://hadoop162:8123', " +
                            "   'database-name' = 'gmall2021', " +
                            "   'table-name' = 'province_stats_2021', " +
                            "   'sink.batch-size' = '5', " +
                            "   'sink.flush-interval' = '2000', " +
                            "   'sink.max-retries' = '3' " +
                            ")");
        // 3. 在动态表A上执行连续查询
        Table table = tEnv
            .sqlQuery("select " +
                          " date_format(tumble_start(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                          " date_format(tumble_end(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                          " province_id, " +
                          " province_name, " +
                          " province_area_code area_code, " +
                          " province_iso_code iso_code, " +
                          " province_3166_2_code iso_3166_2, " +
                          " sum(split_total_amount) order_amount, " +
                          " count(distinct order_id) order_count, " +
                          " unix_timestamp() * 1000 ts " +
                          "from order_wide " +
                          "group by " +
                          " tumble(et, interval '5' second), " +
                          " province_id, " +
                          " province_name," +
                          " province_area_code," +
                          " province_iso_code," +
                          " province_3166_2_code");
    
        // 4. 连续查询的结果写入到动态表B中
        table.executeInsert("province_stats_2021");
        
    }
}
