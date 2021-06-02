package gmallsuger.demo.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import gmallsuger.demo.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/2 15:39
 */

@RestController
public class SugarController {
    
    @Autowired
    ProductStatsService product;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(@RequestParam(value = "date", defaultValue = "0") int date) {
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
    
    @RequestMapping("/sugar/tm")
    public String tm(@RequestParam(value = "date", defaultValue = "0") int date,
                     @RequestParam(value = "limit", defaultValue = "5") int limit) {
        // 如果没有传入日期, 则使用当前日期
        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
    
        List<Map<String, Object>> list = product.getGVMByTM(date, limit);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        // TODO
        for (Map<String, Object> map : list) {
            Object tm_name = map.get("tm_name");
            categories.add(tm_name);
        }
        data.put("categories", categories);
    
        JSONArray series = new JSONArray();
        // TODO
        JSONObject obj = new JSONObject();
        obj.put("name", "商品品牌");
        JSONArray data1 = new JSONArray();
        for (Map<String, Object> map : list) {
            Object oderAmount = map.get("order_amount");
            data1.add(oderAmount);
        }
        obj.put("data", data1);
    
        series.add(obj);
        data.put("series", series);
        result.put("data", data);
    
        return result.toJSONString();
    }
    
}
