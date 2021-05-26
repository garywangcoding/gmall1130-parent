package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
        //        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        
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
        orderInfoStream
            .keyBy(OrderInfo::getId)
            .intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                @Override
                public void processElement(OrderInfo left,
                                           OrderDetail right,
                                           Context ctx,
                                           Collector<OrderWide> out) throws Exception {
                    System.out.println(ctx.getLeftTimestamp());
                    
                    out.collect(new OrderWide(left, right));
                }
            })
            .print();
    }
}
