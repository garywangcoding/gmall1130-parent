package com.atguigu.realtime.app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:29
 */
public abstract class BaseAppSQL {
    
    
    protected abstract void run(StreamTableEnvironment tEnv);
    public void init(int port,
                     int parallelism,
                     String ck) {
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
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ck);
        run(tEnv);
        try {
            env.execute(ck);  // 使用ck作为jobName
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
