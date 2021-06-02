package gmallsuger.demo.controller;

import com.alibaba.fastjson.JSONObject;
import gmallsuger.demo.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:39
 */

@RestController
public class SugarController {
    
    @Autowired
    ProductStatsService product;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(@RequestParam(value = "date" , defaultValue = "0") int date) {
        // 如果没有传入日期, 则使用当前日期
        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
        
        BigDecimal gmv = product.getGMV(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);
    
        return result.toJSONString();
    }
}
