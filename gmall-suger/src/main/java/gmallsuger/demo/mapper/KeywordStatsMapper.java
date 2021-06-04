package gmallsuger.demo.mapper;

import gmallsuger.demo.bean.KeywordStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:29
 */
public interface KeywordStatsMapper {
    @Select("SELECT\n" +
        "    keyword,\n" +
        "    sum(ct * multiIf(source = 'source', 10, source = 'click', 8, source = 'order', 5, 2)) score\n" +
        "FROM keyword_stats_2021\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY keyword\n")
    List<KeywordStats> getKeywordStats(int date);
    
    
   
    
    
}
