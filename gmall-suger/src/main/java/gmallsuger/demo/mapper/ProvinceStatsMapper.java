package gmallsuger.demo.mapper;

import gmallsuger.demo.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:29
 */
public interface ProvinceStatsMapper {
    @Select("SELECT\n" +
        "    province_name,\n" +
        "    sum(order_amount) AS order_amount,\n" +
        "    sum(order_count) AS order_count\n" +
        "FROM province_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY province_name\n")
    List<ProvinceStats> getProvinceStatsByName(int date);  // 计算指定日志的销售额   20210602
    
}
