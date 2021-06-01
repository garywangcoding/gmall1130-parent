package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.app.function.DimAsyncFunction;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.bean.ProductStats;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyDimUtil;
import com.atguigu.realtime.util.MySinkUtil;
import com.atguigu.realtime.util.MyTimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        
        // 多个流union在一起
        DataStream<ProductStats> productStatsStream = parseStreamsAndUion(dsMap);
        
        // 添加水印, 开窗, 聚合
        SingleOutputStreamOperator<ProductStats> aggregateResult = aggregateByWindowAndSkuId(productStatsStream);
        
        // join 维度信息
        SingleOutputStreamOperator<ProductStats> resultStream = joinDim(aggregateResult);
        
        // 写入到clickhouse
        sendToClickhouse(resultStream);
        
    }
    
    private void sendToClickhouse(SingleOutputStreamOperator<ProductStats> resultStream) {
        resultStream
            .addSink(MySinkUtil.getClickHouseSink(CLICKHOUSE_DB, Constant.CLICKHOSUE_TABLE_PRODUCT_STATS_2021, ProductStats.class));
    }
    
    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> aggregateResult) {
        return AsyncDataStream.unorderedWait(
            aggregateResult,
            new DimAsyncFunction<ProductStats>() {
                @Override
                protected void addDim(Connection conn,
                                      Jedis client,
                                      ProductStats input,
                                      ResultFuture<ProductStats> resultFuture) throws Exception {
                    // 1. skuInfo
                    JSONObject skuInfo = MyDimUtil.readDim(conn, client, DIM_SKU_INFO, input.getSku_id());
                    input.setSku_name(skuInfo.getString("SKU_NAME"));
                    input.setSku_price(skuInfo.getBigDecimal("PRICE"));
                    
                    input.setSpu_id(skuInfo.getLong("SPU_ID"));
                    input.setTm_id(skuInfo.getLong("TM_ID"));
                    input.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                    
                    // 2. 补齐spu_info
                    JSONObject spuInfo = MyDimUtil.readDim(conn, client, DIM_SPU_INFO, input.getSpu_id());
                    input.setSpu_name(spuInfo.getString("SPU_NAME"));
                    
                    // 3. 补齐 tm
                    JSONObject tmInfo = MyDimUtil.readDim(conn, client, DIM_BASE_TRADEMARK, input.getTm_id());
                    input.setTm_name(tmInfo.getString("TM_NAME"));
                    
                    // 4. 补齐category3
                    JSONObject c3 = MyDimUtil.readDim(conn, client, DIM_BASE_CATEGORY3, input.getCategory3_id());
                    input.setCategory3_name(c3.getString("NAME"));
                    
                    resultFuture.complete(Collections.singletonList(input));
                }
            },
            30,
            TimeUnit.SECONDS
        );
    }
    
    private SingleOutputStreamOperator<ProductStats> aggregateByWindowAndSkuId(DataStream<ProductStats> productStatsStream) {
        return productStatsStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((ps, ts) -> ps.getTs())
            
            )
            .keyBy(ProductStats::getSku_id)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1,
                                               ProductStats stats2) throws Exception {
                        
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        
                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        
                        return stats1;
                    }
                },
                new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key,
                                      TimeWindow window,
                                      Iterable<ProductStats> input,
                                      Collector<ProductStats> out) throws Exception {
                        ProductStats ps = input.iterator().next();
                        
                        ps.setStt(MyTimeUtil.tsToDateTimeString(window.getStart()));
                        ps.setEdt(MyTimeUtil.tsToDateTimeString(window.getEnd()));
                        
                        ps.setOrder_ct((long) ps.getOrderIdSet().size());
                        ps.setPaid_order_ct((long) ps.getPaidOrderIdSet().size());
                        ps.setRefund_order_ct((long) ps.getRefundOrderIdSet().size());
                        
                        out.collect(ps);
                        
                    }
                }
            );
    }
    
    private DataStream<ProductStats> parseStreamsAndUion(Map<String, DataStreamSource<String>> dsMap) {
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
                    // 只有是点击了某个商品的连接, 才算这个商品被点击. 这个日志的特点 : page_id的值是 good_detail
                    if ("good_detail".equals(pageId)) {
                        ProductStats ps = new ProductStats();
                        ps.setSku_id(page.getLong("item"));
                        ps.setTs(input.getLong("ts"));
                        ps.setClick_ct(1L);
                        
                        out.collect(ps);
                        
                    }
                    
                }
            });
        
        // 3. 收藏
        SingleOutputStreamOperator<ProductStats> productFavorStatsStream = dsMap.get(DWD_FAVOR_INFO)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(obj.getLong("sku_id"));
                ps.setTs(MyTimeUtil.dateTimeToTs(obj.getString("create_time")));
                ps.setFavor_ct(1L);
                return ps;
            });
        
        // 4. 购物车
        SingleOutputStreamOperator<ProductStats> productCartStatsStream = dsMap.get(DWD_CART_INFO)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(obj.getLong("sku_id"));
                ps.setTs(MyTimeUtil.dateTimeToTs(obj.getString("create_time")));
                ps.setCart_ct(1L);
                return ps;
            });
        
        // 5. 下单
        SingleOutputStreamOperator<ProductStats> productOrderStatsStream = dsMap.get(DWM_ORDER_WIDE)
            .map(json -> {
                OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(orderWide.getSku_id());
                ps.setTs(MyTimeUtil.dateTimeToTs(orderWide.getCreate_time()));
                ps.setOrder_amount(orderWide.getSplit_total_amount());
                ps.setOrder_sku_num(orderWide.getSku_num());
                
                //用来保存订单id, 等聚合后再去统计这个订单的个数
                ps.getOrderIdSet().add(orderWide.getOrder_id());
                
                return ps;
            });
        
        // 6. 支付
        SingleOutputStreamOperator<ProductStats> productPaymentStatsStream = dsMap.get(DWM_PAYMENT_WIDE)
            .map(json -> {
                PaymentWide pw = JSON.parseObject(json, PaymentWide.class);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(pw.getSku_id());
                ps.setTs(MyTimeUtil.dateTimeToTs(pw.getPayment_create_time()));
                ps.setPayment_amount(pw.getSplit_total_amount());
                
                ps.getPaidOrderIdSet().add(pw.getOrder_id());
                
                return ps;
            });
        
        // 7. 退款
        SingleOutputStreamOperator<ProductStats> productRefundStatsStream = dsMap.get(DWD_ORDER_REFUND_INFO)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(obj.getLong("sku_id"));
                ps.setTs(MyTimeUtil.dateTimeToTs(obj.getString("create_time")));
                ps.setRefund_amount(obj.getBigDecimal("refund_amount"));
                ps.getRefundOrderIdSet().add(obj.getLong("order_id"));
                return ps;
            });
        
        // 8. 评价
        SingleOutputStreamOperator<ProductStats> productCommentStatsStream = dsMap.get(DWD_COMMENT_INFO)
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                
                ProductStats ps = new ProductStats();
                ps.setSku_id(obj.getLong("sku_id"));
                ps.setComment_ct(1L);
                ps.setTs(MyTimeUtil.dateTimeToTs(obj.getString("create_time")));
                
                String appraise = obj.getString("appraise");
                if (GOOD_COMMENT_FIVE_STARS.equals(appraise) || GOOD_COMMENT_FOUR_STARS.equals(appraise)) {
                    ps.setGood_comment_ct(1L);
                }
                
                return ps;
            });
        
        // 9. union 在一起
        return productDisplayStream.union(productClickStatsStream,
                                          productFavorStatsStream,
                                          productCartStatsStream,
                                          productOrderStatsStream,
                                          productPaymentStatsStream,
                                          productRefundStatsStream,
                                          productCommentStatsStream);
        
    }
}
