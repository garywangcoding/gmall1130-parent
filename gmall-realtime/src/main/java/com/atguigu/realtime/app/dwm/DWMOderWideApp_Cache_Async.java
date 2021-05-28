package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/25 15:34
 */
public class DWMOderWideApp_Cache_Async extends BaseAppV2 {
    public static void main(String[] args) {
        new DWMOderWideApp_Cache_Async().init(3003,
                                              2,
                                              "DWMOderWideApp_Cache",
                                              "DWMOderWideApp_Cache",
                                              Constant.DWD_ORDER_INFO,
                                              Constant.DWD_ORDER_DETAIL);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> dsMap) {
        
        // 1. 事实表的join
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDim = factJoin(dsMap);
        // 2. join 维度表
        SingleOutputStreamOperator<OrderWide> orderWideWithDim = joinDim(orderWideWithoutDim);
        // 3. 最后数据写入到Kafka中(dwm_order_wide)
        sendToKafka(orderWideWithDim);
    }
    
    private void sendToKafka(SingleOutputStreamOperator<OrderWide> orderWideWithDim) {
        orderWideWithDim
            .addSink(MyKafkaUtil.getKafkaSink(Constant.DWM_ORDER_WIDE));
    }
    
    private SingleOutputStreamOperator<OrderWide> joinDim(SingleOutputStreamOperator<OrderWide> orderWideWithoutDim) {
        return AsyncDataStream.unorderedWait(
            orderWideWithoutDim,
            new RichAsyncFunction<OrderWide, OrderWide>() {
                
                private Connection conn;
                private ThreadPoolExecutor pool;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    pool = MyThreadPoolUtil.getThreadPool();
                    conn = MyJdbcUtil.getConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
                }
                
                @Override
                public void asyncInvoke(OrderWide orderWide,
                                        ResultFuture<OrderWide> resultFuture) throws Exception {
                    pool.execute(new Runnable() {
                        @Override
                        public void run() {
                            // 一个异步操作, 一个redis的客户端
                            Jedis client = MyRedisUtil.getRedisClient();
                            try {
                                // 1. 读取用户维度信息
                                JSONObject userInfo = MyDimUtil.readDim(conn, client, Constant.DIM_USER_INFO, orderWide.getUser_id());
                                
                                orderWide.setUser_age(userInfo.getString("BIRTHDAY"));
                                
                                orderWide.setUser_gender(userInfo.getString("GENDER"));
                                
                                // 2. join省份信息
                                JSONObject province = MyDimUtil.readDim(conn, client, Constant.DIM_BASE_PROVINCE, orderWide.getProvince_id());
                                orderWide.setProvince_3166_2_code(province.getString("ISO_3166_2"));
                                orderWide.setProvince_area_code(province.getString("AREA_CODE"));
                                orderWide.setProvince_iso_code(province.getString("ISO_CODE"));
                                orderWide.setProvince_name(province.getString("NAME"));
                                
                                // 3. join sku info
                                JSONObject skuInfo = MyDimUtil.readDim(conn, client, Constant.DIM_SKU_INFO, orderWide.getSku_id());
                                orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                                orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                                orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                                orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                                
                                // 4. join spu info
                                JSONObject spuInfo = MyDimUtil.readDim(conn, client, Constant.DIM_SPU_INFO, orderWide.getSpu_id());
                                orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                                
                                // 5. join tm
                                JSONObject tm = MyDimUtil.readDim(conn, client, Constant.DIM_BASE_TRADEMARK, orderWide.getTm_id());
                                orderWide.setTm_name(tm.getString("TM_NAME"));
                                
                                // 6. join c3
                                JSONObject c3 = MyDimUtil.readDim(conn, client, Constant.DIM_BASE_CATEGORY3, orderWide.getCategory3_id());
                                orderWide.setCategory3_name(c3.getString("NAME"));
                                
                                // 把结果写入到 resultFuture 中
                                resultFuture.complete(Collections.singletonList(orderWide));
                                
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            // 把客户端还给连接池
                            client.close();
                            
                        }
                    });
                    
                }
            },
            120,
            TimeUnit.SECONDS);
    }
    
    private SingleOutputStreamOperator<OrderWide> factJoin(Map<String, DataStreamSource<String>> dsMap) {
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = dsMap
            .get(Constant.DWD_ORDER_INFO)
            .map(json -> JSON.parseObject(json, OrderInfo.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((orderInfo, ts) -> orderInfo.getCreate_ts())
            );
        SingleOutputStreamOperator<OrderDetail> orderDetailStream = dsMap
            .get(Constant.DWD_ORDER_DETAIL)
            .map(json -> JSON.parseObject(json, OrderDetail.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((orderDetail, ts) -> orderDetail.getCreate_ts())
            );
        
        // 1. 进行双流join
        return orderInfoStream
            .keyBy(OrderInfo::getId)
            .intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                @Override
                public void processElement(OrderInfo left,
                                           OrderDetail right,
                                           Context ctx,
                                           Collector<OrderWide> out) throws Exception {
                    //System.out.println(ctx.getLeftTimestamp());
                    
                    out.collect(new OrderWide(left, right));
                }
            });
    }
}
/*
优化措施1: 加缓存
缓存位置: redis
存储的数据: 6个维度表的数据
redis的数据结构:(string list set hash zset )

从redis取维度数据的时候, 依据什么条件?
    表名 还有 对应的 id
--------------------------
key       value( hash)
""        field     value

"表名"     id        {"": ""} json格式的字符串

好处: 存取方便, key和维度表的个数一致, 方管理
缺点: 没有办法单独对每个维度行数据做过期时间的管理

---------------------------
key         value(string)
"表名:id"   json 格式的字符串

好处: 方便存取, 可以单独给每个维度行设置过期时间
坏处: key特别多.  单独放在一个独立的数据库中

 



 */
