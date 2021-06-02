package gmallsuger.demo.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public interface ProductStatsService {
    BigDecimal getGMV(int date);
    
    List<Map<String, Object>> getGVMByTM(int date, int limit);
}
