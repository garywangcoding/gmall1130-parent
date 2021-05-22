package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:19
 */
public class MyKafkaUtil {
    
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId,
                                                            String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("isolation.level", "read_committed");
        
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
    
    public static SinkFunction<JSONObject> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");
        return new FlinkKafkaProducer<JSONObject>(
            topic,
            new KafkaSerializationSchema<JSONObject>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                    return new ProducerRecord<>(topic, null, element.toJSONString().getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
