package gmallsuger.demo.service;

import gmallsuger.demo.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    
    @Autowired
    ProductStatsMapper product;
    
    @Override
    public BigDecimal getGMV(int date) {
        return product.getGMV(date);
    }
}
