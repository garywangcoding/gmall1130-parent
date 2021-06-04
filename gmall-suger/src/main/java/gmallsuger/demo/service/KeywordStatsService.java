package gmallsuger.demo.service;

import gmallsuger.demo.bean.KeywordStats;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(int date);
}
