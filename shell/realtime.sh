#!/bin/bash
flink_home=/opt/module/flink-yarn
realtime_jar=/opt/gmall1130/gmall-realtime-1.0-SNAPSHOT.jar
dwd_log_class=com.atguigu.realtime.app.dwd.DWDLogApp
# 启动dwd层的日志分流

function run(){
 $flink_home/bin/flink run -d -c $1 $realtime_jar
}

echo "启动dwd层日志分流"
run $dwd_log_class
