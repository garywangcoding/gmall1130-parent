package gmallsuger.demo.service;

import gmallsuger.demo.bean.ProvinceStats;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStatsByName(int date);  // 计算指定日志的销售额   20210602
    
}
