package com.atguigu.realtime.common;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:24
 */
public class Constant {
    public static final String ODS_LOG = "ods_log";
    public static final String ODS_DB = "ods_db";
    public static final String DWD_PAGE_LOG = "dwd_page_log";
    public static final String DWD_START_LOG = "dwd_start_log";
    public static final String DWD_DISPLAY_LOG = "dwd_display_log";
    
    public static final String DWM_UV = "dwm_uv";
    public static final String DWM_JUMP_DETAIL = "dwm_jump_detail";
    public static final String DWD_ORDER_INFO = "dwd_order_info";
    public static final String DWD_ORDER_DETAIL = "dwd_order_detail";
    public static final String DWM_ORDER_WIDE = "dwm_order_wide";  // alt+return
    public static final String DWD_FAVOR_INFO = "dwd_favor_info";  // alt+return
    public static final String DWD_CART_INFO = "dwd_cart_info";  // alt+return
    public static final String DWD_ORDER_REFUND_INFO = "dwd_order_refund_info";  // alt+return
    public static final String DWD_COMMENT_INFO = "dwd_comment_info";  // alt+return
    
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    
    public static final String DIM_USER_INFO = "DIM_USER_INFO";
    public static final String DIM_SPU_INFO = "DIM_SPU_INFO";
    public static final String DIM_SKU_INFO = "DIM_SKU_INFO";
    public static final String DIM_BASE_TRADEMARK = "DIM_BASE_TRADEMARK";
    public static final String DIM_BASE_PROVINCE = "DIM_BASE_PROVINCE";
    public static final String DIM_BASE_CATEGORY3 = "DIM_BASE_CATEGORY3";
    
    public static final String DWD_PAYMENT_INFO = "dwd_payment_info";
    public static final String DWM_PAYMENT_WIDE = "dwm_payment_wide";
    public static final String CLICKHOUSE_DB = "gmall2021";
    public static final String CLICKHOSUE_TABLE_VISITOR_STATS_2021 = "visitor_stats_2021";
    public static final String CLICKHOSUE_TABLE_PRODUCT_STATS_2021 = "product_stats_2021";
    
    // 如果向让url指定倒具体的数据, 后面再追加数据库  CLICKHOSUE_URL_PRE + CLICKHOUSE_DB
    public static final String CLICKHOSUE_URL_PRE = "jdbc:clickhouse://hadoop162:8123/";
    public static final String CLICKHOSUE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    
    public static final String GOOD_COMMENT_FIVE_STARS = "1205";
    public static final String GOOD_COMMENT_FOUR_STARS = "1204";
    
    public static final String SOURCE_SEARCH = "search";
    public static final String SOURCE_CLICK = "click";
    public static final String SOURCE_ORDER = "order";
    public static final String SOURCE_CART = "cart";
    
    
    public static final String DWS_PRODUCT_STATS = "DWS_PRODUCT_STATS";
    
}
