package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/21 10:11
 */
/*@Controller
@ResponseBody*/
@RestController
@Slf4j
public class LoggerController {
    
    @RequestMapping("/applog")
    public String log(@RequestParam("param") String log) {
        // 1. 把数据写入到磁盘
        save2Disk(log);
        
        // 2. 把数据写入到Kafka
        send2kafka(log);
        
        return "ok";
    }
    
    @Autowired
    KafkaTemplate<String, String> kafka;
    private void send2kafka(String log) {
        kafka.send("ods_log", log);
    }
    
    private void save2Disk(String strLog) {
        log.info(strLog);
    }
}
