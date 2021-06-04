package gmallsuger.demo.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import gmallsuger.demo.bean.ProvinceStats;
import gmallsuger.demo.service.ProductStatsService;
import gmallsuger.demo.service.ProvinceStatsService;
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
    
    @RequestMapping("/sugar/spu")
    public String spu(@RequestParam(value = "date", defaultValue = "0") int date,
                     @RequestParam(value = "limit", defaultValue = "5") int limit) {
        // 如果没有传入日期, 则使用当前日期
        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
    
        List<Map<String, Object>> list = product.getGVMBySPU(date, limit);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        JSONArray data = new JSONArray();
        for (Map<String, Object> map : list) {
            Object spuName = map.get("spu_name");
            Object value = map.get("order_amount");
    
            JSONObject obj = new JSONObject();
            obj.put("name", spuName);
            obj.put("value", value);
            data.add(obj);
        }
        
        result.put("data", data);
    
        return result.toJSONString();
    }
    
    @RequestMapping("/sugar/c3")
    public String c3(@RequestParam(value = "date", defaultValue = "0") int date,
                      @RequestParam(value = "limit", defaultValue = "5") int limit) {
        // 如果没有传入日期, 则使用当前日期
        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
    
        List<Map<String, Object>> list = product.getGVMByC3(date, limit);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject c1 = new JSONObject();
        c1.put("name", "三级品类");
        c1.put("id", "c3_name");
        columns.add(c1);
    
        JSONObject c2 = new JSONObject();
        c2.put("name", "销售额");
        c2.put("id", "c3_amount");
        columns.add(c2);
        
        JSONObject c3 = new JSONObject();
        c3.put("name", "订单数");
        c3.put("id", "c3_count");
        columns.add(c3);
        data.put("columns", columns);
    
        JSONArray rows = new JSONArray();
        
        
        data.put("rows", rows);
        for (Map<String, Object> map : list) {
            JSONObject row = new JSONObject();
            row.put("c3_name", map.get("category3_name"));
            row.put("c3_amount", map.get("order_amount"));
            row.put("c3_count", map.get("order_ct"));
            rows.add(row);
        }
        result.put("data", data);
    
        return result.toJSONString();
    }
    
    
    
    @Autowired
    ProvinceStatsService province;
    @RequestMapping("/sugar/province")
    public String province(@RequestParam(value = "date", defaultValue = "0") int date) {
        // 如果没有传入日期, 则使用当前日期
        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }
    
        List<ProvinceStats> list = province.getProvinceStatsByName(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        JSONObject data = new JSONObject();
        JSONArray mapData = new JSONArray();
        for (ProvinceStats ps : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", ps.getProvince_name());
            obj.put("value", ps.getOrder_amount());
            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(ps.getOrder_count());
            obj.put("tooltipValues", tooltipValues);
            
            mapData.add(obj);
        }
        
        data.put("mapData", mapData);
    
        data.put("valueName", "销售额");
        
    
        JSONArray tooTipNames = new JSONArray();
        tooTipNames.add("订单数2");
        data.put("tooltipNames", tooTipNames);
        
    
        result.put("data", data);
    
        return result.toJSONString();
    }
    
    
}
