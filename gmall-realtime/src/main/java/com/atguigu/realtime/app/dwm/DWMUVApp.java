package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyKafkaUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/25 10:00
 */
public class DWMUVApp extends BaseApp {
    public static void main(String[] args) {
        new DWMUVApp().init(3001, 2, "DWMUVApp", "DWMUVApp", Constant.DWD_PAGE_LOG);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        sourceStream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                
                private ValueState<Long> firstVisitState;
                private SimpleDateFormat simpleDateFormat;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstVisitState", Long.class));
                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                }
                
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<JSONObject> elements,
                                    Collector<JSONObject> out) throws Exception {
                    String current = simpleDateFormat.format(new Date(ctx.window().getEnd()));
                    
                    // 当第二天的时候, 把状态清空
                    // 1. 使用定时器: 明天0:0:0 清空
                    // 2. 通过比较日期是否发生变化, 如果有变化,则预示新的一天来了
                    if (firstVisitState.value() == null
                        || !current.equals(simpleDateFormat.format(new Date(firstVisitState.value())))) {
                        ArrayList<JSONObject> list = Lists.newArrayList(elements);
                        // 时间戳最小的那条记录, 就是当天的第一条日志
                        JSONObject obj = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                        out.collect(obj);
                        
                        firstVisitState.update(obj.getLong("ts"));
                    }
                }
            })
            .addSink(MyKafkaUtil.getKafkaSink(Constant.DWM_UV));
        
    }
}
