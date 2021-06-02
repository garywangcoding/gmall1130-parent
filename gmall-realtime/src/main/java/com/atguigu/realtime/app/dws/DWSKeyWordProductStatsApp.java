package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppSQL;
import com.atguigu.realtime.app.function.KeyWordProductUdtf;
import com.atguigu.realtime.app.function.KeyWordUdtf;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/29 15:15
 */
public class DWSKeyWordProductStatsApp extends BaseAppSQL {
    public static void main(String[] args) {
        new DWSKeyWordProductStatsApp().init(4005, 1, "DWSKeyWordProductStatsApp");
    }
    
    @Override
    protected void run(StreamTableEnvironment tEnv) {
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));
        
        // 建表和kafka关联
        tEnv.executeSql("CREATE TABLE dws_product_stats (" +
                            "   stt string," +
                            "   edt string," +
                            "   spu_name string, " +
                            "   click_ct bigint, " +
                            "   order_ct bigint , " +
                            "   cart_ct bigint " +
                            ") WITH(" +
                            "   'connector' = 'kafka'," +
                            "   'topic' = '" + Constant.DWS_PRODUCT_STATS + "'," +
                            "   'properties.bootstrap.servers' = 'hadoop162:9029,hadoop163:9092,hadoop164:9092'," +
                            "   'properties.group.id' = 'DWSKeyWordProductStatsApp'," +
                            "   'scan.startup.mode' = 'latest-offset'," +
                            "   'format' = 'json'" +
                            ")");
        // 建表和clickhouse关联
        tEnv.executeSql("create table keyword_stats_2021(" +
                            "   stt string," +
                            "   edt string," +
                            "   keyword string," +
                            "   source string," +
                            "   ct bigint," +
                            "   ts bigint," +
                            "   PRIMARY KEY (stt, edt, keyword, source) NOT ENFORCED" +
                            ")with(" +
                            "   'connector' = 'clickhouse', " +
                            "   'url' = 'clickhouse://hadoop162:8123', " +
                            "   'database-name' = 'gmall2021', " +
                            "   'table-name' = 'keyword_stats_2021'," +
                            "   'sink.batch-size' = '100', " +
                            "   'sink.flush-interval' = '2000', " +
                            "   'sink.max-retries' = '3' " +
                            ")");
        
        tEnv.createTemporaryFunction("ik_analyzer", KeyWordUdtf.class);
        
        Table t1 = tEnv.sqlQuery("select" +
                                     "   kw, " +
                                     "   stt," +
                                     "   edt," +
                                     "   click_ct, " +
                                     "   order_ct , " +
                                     "   cart_ct " +
                                     "from dws_product_stats " +
                                     "join lateral table(ik_analyzer(spu_name)) as T(kw) on true ");
        tEnv.createTemporaryView("t1", t1);
        
        // 聚合
        
        Table t2 = tEnv.sqlQuery("select " +
                                     "kw," +
                                     "stt," +
                                     "edt, " +
                                     "sum(click_ct) click_ct, " +
                                     "sum(order_ct) order_ct, " +
                                     "sum(cart_ct) cart_ct " +
                                     "from t1 " +
                                     "where click_ct >0 or order_ct >0 or cart_ct >0 " +
                                     "group by kw, stt, edt ");
        tEnv.createTemporaryView("t2", t2);
        
        // 把三个指标变成3行
        tEnv.createTemporaryFunction("kw_product", KeyWordProductUdtf.class);
        tEnv
            .sqlQuery("select " +
                          " stt," +
                          " edt," +
                          " kw keyword, " +
                          " source, " +
                          " ct, " +
                          " unix_timestamp() * 1000 ts " +
                          "from t2, " +
                          "lateral table(kw_product(click_ct, order_ct, cart_ct))  ")
            .executeInsert("keyword_stats_2021");
            /*.execute()
            .print();  // Row<source, count>*/
        
    }
}
