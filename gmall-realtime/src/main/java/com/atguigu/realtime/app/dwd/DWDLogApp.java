package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyCommonUtil;
import com.atguigu.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:18
 */
public class DWDLogApp extends BaseApp {
    public static void main(String[] args) {
        new DWDLogApp().init(2001, 2, "DWDLogApp", "DWDLogApp", Constant.ODS_LOG);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
        // 1. 识别新老访客
        SingleOutputStreamOperator<JSONObject> validateFlatStream = distinguishNewOrOld(sourceStream);
        
        // 2. 分流
        Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> threeStream = splitStream(validateFlatStream);
        
        // 3. 把数据写入到dwd层(kafka)
        sendToKafka(threeStream);
    }
    
    private void sendToKafka(Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> threeStream) {
        threeStream.f0.addSink(MyKafkaUtil.getKafkaSink(Constant.DWD_START_LOG));
        threeStream.f1.addSink(MyKafkaUtil.getKafkaSink(Constant.DWD_PAGE_LOG));
        threeStream.f2.addSink(MyKafkaUtil.getKafkaSink(Constant.DWD_DISPLAY_LOG));
    }
    
    private Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        /*
        分流:
            使用侧输出流, 把不同的日志放入不同的流, 最后每个流都会把数据写入到kafka中(dwd)
         */
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {};
        SingleOutputStreamOperator<JSONObject> startLogStream = stream
            .process(new ProcessFunction<JSONObject, JSONObject>() {
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    // 1. 主流: 启动日志  2. 侧输出流: 页面日志 , 曝光日志
                    JSONObject start = value.getJSONObject("start");
                    if (start != null) {
                        // 启动日志
                        out.collect(value);
                    } else {
                        // 1. 如果是页面
                        JSONObject page = value.getJSONObject("page");
                        if (page != null) {
                            
                            ctx.output(pageTag, value);
                        }
                        // 2. 如是曝光
                        
                        JSONArray displays = value.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                Long ts = value.getLong("ts");
                                display.put("ts", ts); // 给曝光日志添加时间戳
                                display.putAll(value.getJSONObject("common")); // 把common的内容也追加到曝光日志中
                                display.put("page_id", value.getJSONObject("page").getString("page_id"));  // 把pageId也添加到曝光日志中
                                ctx.output(displayTag, display);
                                
                            }
                        }
                    }
                }
            });
        
        DataStream<JSONObject> pageStream = startLogStream.getSideOutput(pageTag);
        DataStream<JSONObject> displayStream = startLogStream.getSideOutput(displayTag);
        
        return Tuple3.of(startLogStream, pageStream, displayStream);
        
    }
    
    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> sourceStream) {
        /*
        1. 找到这个用户的第一条访问记录. 考虑到数据的乱序, 使用事件时间
        
        2. 开窗.
        
        3. 对某个用户的第一个窗口内的时间最早那条数据设置为新访客, 其他的应该都是老访客
            如果识别用户的第一个窗口:
                使用状态, 如果状态是空, 则是第一个窗口
                否则就不是第一个窗口
        
        4. 对这个用户后面的其他所有窗口应该都标记为老访客
         */
        return sourceStream
            .map((JSON::parseObject))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                
                private ValueState<Long> firstVisitState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstVisitState", Long.class));
                }
                
                @Override
                public void process(String key,
                                    Context context,
                                    Iterable<JSONObject> elements,
                                    Collector<JSONObject> out) throws Exception {
                    // 纠正每条数据的is_new字段
                    if (firstVisitState.value() == null) {
                        // 这个用户的第一个窗口
                        List<JSONObject> objList = MyCommonUtil.iterableToList(elements);
                        objList.sort(Comparator.comparing(o -> o.getLong("ts")));
                        
                        for (int i = 0; i < objList.size(); i++) {
                            if (i == 0) {
                                objList.get(i).getJSONObject("common").put("is_new", "1");
                                firstVisitState.update(objList.get(i).getLong("ts")); // 更新状态
                                System.out.println(objList.get(i).getJSONObject("common").getString("mid"));
                            } else {
                                objList.get(i).getJSONObject("common").put("is_new", "0");
                            }
                            out.collect(objList.get(i));
                        }
                        
                    } else {
                        // 不是这个用户的第一个窗口
                        for (JSONObject obj : elements) {
                            
                            obj.getJSONObject("common").put("is_new", "0");
                            out.collect(obj);
                        }
                    }
                }
            });
        
    }
}
