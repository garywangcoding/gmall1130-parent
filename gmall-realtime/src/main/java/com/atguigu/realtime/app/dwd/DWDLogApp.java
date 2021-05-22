package com.atguigu.realtime.app.dwd;

import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:18
 */
public class DWDLogApp extends BaseApp {
    public static void main(String[] args) {
        new DWDLogApp().init(2001, 2, "DWDLogApp", "DWDLogApp", Constant.ODS_LOG);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
        sourceStream.print();
    }
}
