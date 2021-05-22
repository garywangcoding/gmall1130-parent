package com.atguigu.realtime.app;

import com.atguigu.realtime.util.MyKafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:29
 */
public abstract class BaseApp {
    protected abstract void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream);
    
    public void init(int port,
                     int parallelism,
                     String ck,
                     String groupId,
                     String topic) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/dw/" + ck));
        env.enableCheckpointing(6000);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env
            .getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        DataStreamSource<String> sourceStream = env.addSource(MyKafkaUtil.getKafkaSource(groupId, topic));
        run(env, sourceStream);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
