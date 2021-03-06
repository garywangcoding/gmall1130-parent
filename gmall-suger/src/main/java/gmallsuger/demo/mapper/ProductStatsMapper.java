package gmallsuger.demo.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

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
    
    // 按照品牌
    @Select("SELECT\n" +
        "    tm_name,\n" +
        "    sum(order_amount) order_amount\n" +
        "FROM product_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY tm_name\n" +
        "order by order_amount desc\n" +
        "LIMIT #{limit}\n")
    List<Map<String, Object>> getGVMByTM(@Param("date") int date, @Param("limit") int limit);
    
    
    @Select("SELECT\n" +
        "    spu_name,\n" +
        "    sum(order_amount) order_amount\n" +
        "FROM product_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY spu_name\n" +
        "LIMIT #{limit}")
    List<Map<String, Object>> getGVMBySpu(@Param("date") int date, @Param("limit") int limit);
    
    
    @Select("SELECT\n" +
        "    category3_name,\n" +
        "    sum(order_amount) AS order_amount,\n" +
        "    sum(order_ct) AS order_ct\n" +
        "FROM product_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY category3_name\n" +
        "LIMIT #{limit}")
    List<Map<String, Object>> getGVMByC3(@Param("date") int date, @Param("limit") int limit);
    
    
    
    
    
}
