package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppSQL;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/29 15:15
 */
public class DWSKeyWordStatsApp extends BaseAppSQL {
    public static void main(String[] args) {
        new DWSKeyWordStatsApp().init(4004, 1, "DWSKeyWordStatsApp");
    }
    
    @Override
    protected void run(StreamTableEnvironment tEnv) {
        tEnv.getConfig().getConfiguration().setString("pipeline.name", "DWSKeyWordStatsApp");
        // 建表和kafka关联
        tEnv.executeSql("CREATE TABLE page_view (" +
                            "   common MAP<STRING,STRING>, " +
                            "   page MAP<STRING,STRING>," +
                            "   ts BIGINT, " +
                            "   rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                            "   WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '10' SECOND " +
                            ") WITH(" +
                            "   'connector' = 'kafka'," +
                            "   'topic' = '" + Constant.DWD_PAGE_LOG + "'," +
                            "   'properties.bootstrap.servers' = 'hadoop162:9029,hadoop163:9092,hadoop164:9092'," +
                            "   'properties.group.id' = 'DWSKeyWordStatsApp'," +
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
        
        // 连续查询
        // 1. 对数据做过滤, 找到用户的搜索的关键词
        Table t1 = tEnv.sqlQuery("select " +
                                        " page['item'] kw, " +
                                        " rowtime " +
                                        "from page_view " +
                                        "where page['item'] is not null " +
                                        "and page['page_id']='good_list' ");
        tEnv.createTemporaryView("t1", t1);
        
        // 2. 对用户搜索的关键词进行分词
        // 分词函数是什么函数?
        tEnv.sqlQuery("select" +
                          "w, " +
                          "rowtime " +
                          "from t1 " +
                          "join lateral table(ik_analyzer(kw)) as T(w) on true");
        
    
        // 结果写入到clickhouse中
        
    }
}
