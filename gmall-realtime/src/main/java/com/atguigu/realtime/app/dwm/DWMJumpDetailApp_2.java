package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/25 14:05
 */
public class DWMJumpDetailApp_2 extends BaseApp {
    public static void main(String[] args) {
        // 最终的目的: 是找到跳出的记录, 最后写入到Kafka
        new DWMJumpDetailApp_2().init(3002, 1, "DWMJumpDetailApp_2", "DWMJumpDetailApp_2", Constant.DWD_PAGE_LOG);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
    
        /*sourceStream =
            env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":10000} ",
                
                
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":39999} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":50000} "
            );*/
        
        // 1. 先有流   使用事件时间
        KeyedStream<JSONObject, String> pageStream = sourceStream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        // 2. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
            .<JSONObject>begin("entry")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() == 0;
                }
            })
            .next("second")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() == 0;
                }
            })
            .within(Time.seconds(5));
        // 3. 把模式运用在流上
        PatternStream<JSONObject> ps = CEP.pattern(pageStream, pattern);
        // 4. 从匹配到的模式流中取出需要的数据
        SingleOutputStreamOperator<JSONObject> normal = ps.select(
            new OutputTag<JSONObject>("jump") {},
            new PatternTimeoutFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject timeout(Map<String, List<JSONObject>> map,
                                          long timeoutTimestamp) throws Exception {
                    return map.get("entry").get(0);
                }
            },
            new PatternSelectFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                    // 正常的不需要,可以不用实现
                    return map.get("entry").get(0);
                }
            }
        );
        
        DataStream<JSONObject> jump = normal.getSideOutput(new OutputTag<JSONObject>("jump") {});
        
        normal
            .union(jump)
            .addSink(MyKafkaUtil.getKafkaSink(Constant.DWM_JUMP_DETAIL));
        //            .print();
        
    }
}
