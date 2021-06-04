package gmallsuger.demo.service;

import gmallsuger.demo.bean.ProvinceStats;
import gmallsuger.demo.mapper.ProvinceStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    
    @Autowired
    ProvinceStatsMapper province;
    @Override
    public List<ProvinceStats> getProvinceStatsByName(int date) {
        return province.getProvinceStatsByName(date);
    }
    
}
