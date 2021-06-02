package gmallsuger.demo.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:29
 */
public interface ProductStatsMapper {
    @Select("select " +
        "sum(order_amount) " +
        "from product_stats_2021 " +
        "where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);  // 计算指定日志的销售额   20210602
}
