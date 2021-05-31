package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.ProductStats;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.atguigu.realtime.common.Constant.*;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/31 14:01
 */
public class DWSProductStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DWSProductStatsApp().init(4002, 1, "DWSProductStatsApp", "DWSProductStatsApp",
                                      DWD_DISPLAY_LOG, DWD_PAGE_LOG,
                                      DWD_FAVOR_INFO, DWD_CART_INFO,
                                      DWM_ORDER_WIDE, DWM_PAYMENT_WIDE,
                                      DWD_ORDER_REFUND_INFO, DWD_COMMENT_INFO);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> dsMap) {
        
        parseStreamsAndUion(dsMap);
        
    }
    
    private void parseStreamsAndUion(Map<String, DataStreamSource<String>> dsMap) {
        // 1. 获取曝光流
        SingleOutputStreamOperator<ProductStats> productDisplayStream = dsMap.get(DWD_DISPLAY_LOG)
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, ProductStats>() {
                @Override
                public void processElement(JSONObject input,
                                           Context ctx,
                                           Collector<ProductStats> out) throws Exception {
                    if ("sku_id".equals(input.getString("item_type"))) {
                        ProductStats ps = new ProductStats();
                        ps.setSku_id(input.getLong("item"));
                        ps.setTs(input.getLong("ts"));
                        ps.setDisplay_ct(1L);
                        out.collect(ps);
                    }
                }
            });
        
        // 2. 商品点击
        SingleOutputStreamOperator<ProductStats> productClickStatsStream = dsMap.get(DWD_PAGE_LOG)
            .process(new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String json,
                                           Context ctx,
                                           Collector<ProductStats> out) throws Exception {
                    JSONObject input = JSON.parseObject(json);
                    JSONObject page = input.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    // 只有是点击了某个商品的连接, 才算这个商品被点击. 这个日志的特点集市 : page_id的值是 good_detail
                    if ("good_detail".equals(pageId)) {
                        ProductStats ps = new ProductStats();
                        ps.setSku_id(page.getLong("item"));
                        ps.setTs(input.getLong("ts"));
                        ps.setClick_ct(1L);
                        
                        out.collect(ps);
                        
                    }
                    
                }
            });
        dsMap.get(DWD_FAVOR_INFO).print(DWD_FAVOR_INFO);
        dsMap.get(DWD_CART_INFO).print(DWD_CART_INFO);
        dsMap.get(DWM_ORDER_WIDE).print(DWM_ORDER_WIDE);
        dsMap.get(DWM_PAYMENT_WIDE).print(DWM_PAYMENT_WIDE);
        dsMap.get(DWD_ORDER_REFUND_INFO).print(DWD_ORDER_REFUND_INFO);
        dsMap.get(DWD_COMMENT_INFO).print(DWD_COMMENT_INFO);
    }
}
