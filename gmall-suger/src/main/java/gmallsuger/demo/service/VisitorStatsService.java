package gmallsuger.demo.service;

import gmallsuger.demo.bean.VisitorStats;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByHour(int date);
    
    List<VisitorStats> getVisitorStatsByIsNew(int date);
}
