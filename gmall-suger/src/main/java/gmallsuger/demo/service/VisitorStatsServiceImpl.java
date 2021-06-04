package gmallsuger.demo.service;

import gmallsuger.demo.bean.VisitorStats;
import gmallsuger.demo.mapper.VisitorStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VisitorStatsServiceImpl implements VisitorStatsService{
    
    
    @Autowired
    VisitorStatsMapper visitor;
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitor.getVisitorStatsByHour(date);
    }
    
    @Override
    public List<VisitorStats> getVisitorStatsByIsNew(int date) {
        return visitor.getVisitorStatsByIsNew(date);
    }
    
}
