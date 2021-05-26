package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyDimUtil;
import com.atguigu.realtime.util.MyJdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/25 15:34
 */
public class DWMOderWideApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DWMOderWideApp().init(3003,
                                  2,
                                  "DWMOderWideApp",
                                  "DWMOderWideApp",
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
        orderWideWithDim.print();
        // 3. 最后数据写入到Kafka中(dwm_order_wide)
    }
    
    private SingleOutputStreamOperator<OrderWide> joinDim(SingleOutputStreamOperator<OrderWide> orderWideWithoutDim) {
       return orderWideWithoutDim
            .map(new RichMapFunction<OrderWide, OrderWide>() {
                
                private Connection conn;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    conn = MyJdbcUtil.getConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
                }
                
                @Override
                public OrderWide map(OrderWide orderWide) throws Exception {
                    // 1. 读取用户维度信息
                    JSONObject userInfo = MyDimUtil.readDim(conn, Constant.DIM_USER_INFO, orderWide.getUser_id());
                    orderWide.setUser_age(userInfo.getString("BIRTHDAY"));
                    orderWide.setUser_gender(userInfo.getString("GENDER"));
                    
                    // 2. join省份信息
                    JSONObject province = MyDimUtil.readDim(conn, Constant.DIM_BASE_PROVINCE, orderWide.getProvince_id());
                    orderWide.setProvince_3166_2_code(province.getString("ISO_3166_2"));
                    orderWide.setProvince_area_code(province.getString("AREA_CODE"));
                    orderWide.setProvince_iso_code(province.getString("ISO_CODE"));
                    orderWide.setProvince_name(province.getString("NAME"));
                    
                    // 3. join sku info
                    JSONObject skuInfo = MyDimUtil.readDim(conn, Constant.DIM_SKU_INFO, orderWide.getSku_id());
                    orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                    orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                    orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                    orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                    
                    // 4. join spu info
                    JSONObject spuInfo = MyDimUtil.readDim(conn, Constant.DIM_SPU_INFO, orderWide.getSpu_id());
                    orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                    
                    // 5. join tm
                    JSONObject tm = MyDimUtil.readDim(conn, Constant.DIM_BASE_TRADEMARK, orderWide.getTm_id());
                    orderWide.setTm_name(tm.getString("TM_NAME"));
                    
                    // 6. join c3
                    JSONObject c3 = MyDimUtil.readDim(conn, Constant.DIM_BASE_CATEGORY3, orderWide.getCategory3_id());
                    orderWide.setCategory3_name(c3.getString("NAME"));
                    
                    return orderWide;
                }
                
                @Override
                public void close() throws Exception {
                    // 释放资源
                    if (conn != null) {
                        conn.close();
                    }
                }
            });
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
