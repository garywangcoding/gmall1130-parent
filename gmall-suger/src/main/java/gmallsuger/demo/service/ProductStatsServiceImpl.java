package gmallsuger.demo.service;

import gmallsuger.demo.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    
    @Autowired
    ProductStatsMapper product;
    
    @Override
    public BigDecimal getGMV(int date) {
        return product.getGMV(date);
    }
    
    @Override
    public List<Map<String, Object>> getGVMByTM(int date, int limit) {
        return product.getGVMByTM(date, limit);
    }
    
    @Override
    public List<Map<String, Object>> getGVMBySPU(int date, int limit) {
        return product.getGVMBySpu(date, limit);
    }
    
    @Override
    public List<Map<String, Object>> getGVMByC3(int date, int limit) {
        return product.getGVMByC3(date, limit);
    }
    
}
