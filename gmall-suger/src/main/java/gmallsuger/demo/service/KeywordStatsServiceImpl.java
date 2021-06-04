package gmallsuger.demo.service;

import gmallsuger.demo.bean.KeywordStats;
import gmallsuger.demo.mapper.KeywordStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    
    @Autowired
    KeywordStatsMapper keyword;
    
    @Override
    public List<KeywordStats> getKeywordStats(int date) {
        return keyword.getKeywordStats(date);
    }
}
