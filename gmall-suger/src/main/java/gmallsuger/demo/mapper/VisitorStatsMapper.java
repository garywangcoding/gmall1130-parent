package gmallsuger.demo.mapper;

import gmallsuger.demo.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:29
 */
public interface VisitorStatsMapper {
    @Select("SELECT\n" +
        "    toHour(stt) AS hour,\n" +
        "    sum(uv_ct) AS uv_ct,\n" +
        "    sum(pv_ct) AS pv_ct,\n" +
        "    sum(uj_ct) AS uj_ct,\n" +
        "    sum(sv_ct) AS sv_ct\n" +
        "FROM visitor_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY toHour(stt)")
    List<VisitorStats> getVisitorStatsByHour(int date);
    
}
