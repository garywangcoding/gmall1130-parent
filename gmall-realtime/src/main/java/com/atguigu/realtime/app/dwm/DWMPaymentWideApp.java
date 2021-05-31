package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentInfo;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyKafkaUtil;
import com.atguigu.realtime.util.MyTimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

import static com.atguigu.realtime.common.Constant.DWD_PAYMENT_INFO;
import static com.atguigu.realtime.common.Constant.DWM_ORDER_WIDE;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/28 10:13
 */
public class DWMPaymentWideApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DWMPaymentWideApp().init(3004, 1, "DWMPaymentWideApp", "DWMPaymentWideApp",
                                     DWD_PAYMENT_INFO,
                                     DWM_ORDER_WIDE);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> dsMap) {
        
        KeyedStream<PaymentInfo, Long> paymentInfoStream = dsMap
            .get(DWD_PAYMENT_INFO)
            .map(json -> JSON.parseObject(json, PaymentInfo.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((obj, ts) -> MyTimeUtil.dateTimeToTs(obj.getCreate_time()))
            )
            .keyBy(PaymentInfo::getOrder_id);
        
        KeyedStream<OrderWide, Long> orderWideStream = dsMap
            .get(DWM_ORDER_WIDE)
            .map(json -> {
                System.out.println(json);
                return JSON.parseObject(json, OrderWide.class);
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((obj, ts) -> MyTimeUtil.dateTimeToTs(obj.getCreate_time()))
            )
            .keyBy(OrderWide::getOrder_id);
        
        paymentInfoStream
            .intervalJoin(orderWideStream)
            .between(Time.minutes(-45), Time.minutes(1))
            .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                @Override
                public void processElement(PaymentInfo left,
                                           OrderWide right,
                                           Context ctx,
                                           Collector<PaymentWide> out) throws Exception {
                    out.collect(new PaymentWide(left, right));
                }
            })
            .addSink(MyKafkaUtil.getKafkaSink(Constant.DWM_PAYMENT_WIDE));
        
    }
}
