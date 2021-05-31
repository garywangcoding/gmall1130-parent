package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.VisitorStats;
import com.atguigu.realtime.util.MySinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Map;

import static com.atguigu.realtime.common.Constant.*;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/29 15:15
 */
public class DWSVisitorStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DWSVisitorStatsApp().init(4001, 1, "DWSVisitorStatsApp", "DWSVisitorStatsApp",
                                      DWD_PAGE_LOG, DWM_UV, DWM_JUMP_DETAIL);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> dsMap) {
        
        // 1. 解析并union
        DataStream<VisitorStats> statsStream = parseStreamsAndUnion(dsMap);
        // 2. 开窗聚合
        SingleOutputStreamOperator<VisitorStats> aggregatedStream = aggregateByWindowAndDim(statsStream);
        
        // 3. 把数据写入到ClickHouse中
        sendToClickHouse(aggregatedStream);
        
    }
    
    private void sendToClickHouse(SingleOutputStreamOperator<VisitorStats> aggregatedStream) {
        aggregatedStream.addSink(MySinkUtil.getClickHouseSink(CLICKHOUSE_DB,
                                                              CLICKHOSUE_TABLE_VISITOR_STATS_2021,
                                                              VisitorStats.class));
    }
    
    private SingleOutputStreamOperator<VisitorStats> aggregateByWindowAndDim(DataStream<VisitorStats> statsStream) {
        return statsStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((ele, ts) -> ele.getTs())
            )
            .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats vs1,
                                               VisitorStats vs2) throws Exception {
                        vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                        vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                        vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                        vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                        vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                        return vs1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<VisitorStats> input,
                                      Collector<VisitorStats> out) throws Exception {
                        VisitorStats vs = input.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        vs.setStt(sdf.format(window.getStart()));
                        vs.setEdt(sdf.format(new Date(window.getEnd())));
                        vs.setTs(window.getEnd());
                        out.collect(vs);
                        
                    }
                }
            );
    }
    
    private DataStream<VisitorStats> parseStreamsAndUnion(Map<String, DataStreamSource<String>> dsMap) {
        
        DataStreamSource<String> pageStream = dsMap.get(DWD_PAGE_LOG);
        DataStreamSource<String> uvStream = dsMap.get(DWM_UV);
        DataStreamSource<String> userJumpStream = dsMap.get(DWM_JUMP_DETAIL);
        
        // 1. 计算pv 和持续时间
        SingleOutputStreamOperator<VisitorStats> pvAndDuringTimeStatsStream = pageStream
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, VisitorStats>() {
                @Override
                public void processElement(JSONObject input,
                                           Context ctx,
                                           Collector<VisitorStats> out) throws Exception {
                    JSONObject common = input.getJSONObject("common");
                    JSONObject page = input.getJSONObject("page");
                    
                    VisitorStats vs = new VisitorStats(
                        "", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 1L, 0L, 0L, page.getLong("during_time"),
                        input.getLong("ts"));
                    out.collect(vs);
                    
                }
            });
        
        // 2. 计算uv
        SingleOutputStreamOperator<VisitorStats> uvStatsStream = uvStream
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, VisitorStats>() {
                @Override
                public void processElement(JSONObject input,
                                           Context ctx,
                                           Collector<VisitorStats> out) throws Exception {
                    JSONObject common = input.getJSONObject("common");
                    
                    VisitorStats vs = new VisitorStats(
                        "", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L, 0L, 0L, 0L, 0L,
                        input.getLong("ts"));
                    
                    out.collect(vs);
                    
                }
            });
        
        // 3. 计算sv 进入次数 数据来源? dwd_page_log
        SingleOutputStreamOperator<VisitorStats> svStatsStream = pageStream
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, VisitorStats>() {
                @Override
                public void processElement(JSONObject input,
                                           Context ctx,
                                           Collector<VisitorStats> out) throws Exception {
                    JSONObject common = input.getJSONObject("common");
                    JSONObject page = input.getJSONObject("page");
                    
                    String lastPageId = page.getString("last_page_id");
                    if (lastPageId == null || lastPageId.isEmpty()) {
                        VisitorStats vs = new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 1L, 0L, 0L,
                            input.getLong("ts"));
                        out.collect(vs);
                    }
                    
                }
            });
        
        // 4. 跳出次数
        SingleOutputStreamOperator<VisitorStats> ujStatsStream = userJumpStream
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, VisitorStats>() {
                @Override
                public void processElement(JSONObject input,
                                           Context ctx,
                                           Collector<VisitorStats> out) throws Exception {
                    JSONObject common = input.getJSONObject("common");
                    VisitorStats vs = new VisitorStats(
                        "", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 0L, 0L, 1L, 0L,
                        input.getLong("ts"));
                    out.collect(vs);
                }
            });
        
        // 把4个流union在一起
        return pvAndDuringTimeStatsStream
            .union(uvStatsStream, svStatsStream, ujStatsStream);
        
    }
}
