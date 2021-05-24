package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.MyPhoenixSinkFunction;
import com.atguigu.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:18
 */
public class DWDDbApp extends BaseApp implements Serializable {
    public static void main(String[] args) {
        new DWDDbApp().init(2002, 2, "DWDDbApp", "DWDDbApp", Constant.ODS_DB);
    }
    
    private OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbaseTag") {};
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> dataStream) {
        // 1. 对业务数据进行etl清洗
        SingleOutputStreamOperator<JSONObject> etledDataStream = etlDataStream(dataStream);
        // 2. 读取配置表的内容(cdc)
        SingleOutputStreamOperator<TableProcess> tableProcessStream = readTableProcess(env);
        
        // 3. 把配置表做成广播流, 与数据进行connect, 利用广播状态来控制每个数据流的流向
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> toKafkaStream = dynamicSplitStream(etledDataStream, tableProcessStream);
        DataStream<Tuple2<JSONObject, TableProcess>> toHbaseStream = toKafkaStream.getSideOutput(hbaseTag);
        
        // 4. 数据sink到正确的位置
        // 4.1 把数据写入到Kafka
        sendToKafka(toKafkaStream);
        // 4.2 把数据写入到HBase中
        sentToHbase(toHbaseStream);
        
        
    }
    
    private void sentToHbase(DataStream<Tuple2<JSONObject, TableProcess>> toHbaseStream) {
        // 自定义sink, 不用从头开始: 可以对 jdbc sink 做封装, 来实现到Phoenix写入
        // 为了提高性能, 最好同一张表的数据进入同一个地方
        toHbaseStream
            .keyBy(t -> t.f1.getSinkTable())
            .addSink(new MyPhoenixSinkFunction());
    }
    
    
    
    
    private void sendToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> toKafkaStream) {
        toKafkaStream.addSink(MyKafkaUtil.getKafkaSink());
    }
    
    // 动态分流
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dynamicSplitStream(SingleOutputStreamOperator<JSONObject> etledDataStream,
                                                                                            SingleOutputStreamOperator<TableProcess> tableProcessStream) {
        MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor =
            new MapStateDescriptor<>("tableProcessState", String.class, TableProcess.class);
        
        // Key的类型: "order_info:insert"   value的类型:
        BroadcastStream<TableProcess> tableProcessBCStream = tableProcessStream
            .broadcast(tableProcessMapStateDescriptor);
        
        return etledDataStream
            .connect(tableProcessBCStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                @Override
                public void processElement(JSONObject value,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    ReadOnlyBroadcastState<String, TableProcess> bdState = ctx.getBroadcastState(tableProcessMapStateDescriptor);
                    
                    String sourceTable = value.getString("table");
                    String operateType = value.getString("type").replaceAll("bootstrap-", "");
                    
                    String key = sourceTable + ":" + operateType;
                    TableProcess tableProcess = bdState.get(key);
                    if (tableProcess != null) {
                        // 开始向后进行处理
                        // 1. 获取真正的数据
                        JSONObject data = value.getJSONObject("data");
                        // 2. 把data中需要的字段保留, 其他的去掉.  由sink_columns来确定
                        filterSinkColumns(data, tableProcess);
                        
                        // 3. 对数据做分流: 主流的进入 Kakfa, 测流就如 Hbase
                        String sinkType = tableProcess.getSinkType();
                        System.out.println(sinkType);
                        if (TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(sinkType)) {
                            out.collect(Tuple2.of(data, tableProcess));
                        } else if (TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(sinkType)) {
                            ctx.output(hbaseTag, Tuple2.of(data, tableProcess));
                        }
                    }
                }
                
                // 过滤掉不需要的字段
                private void filterSinkColumns(JSONObject data, TableProcess tableProcess) {
                    List<String> columns = Arrays.asList(tableProcess.getSinkColumns().split(","));
                    //把data中那些没有出现在columns数组中的key删掉
                    data.keySet().removeIf(key -> !columns.contains(key));  // 删除是原地删除: 直接修改的是原来的集合
                }
                
                @Override
                public void processBroadcastElement(TableProcess value,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 处理广播数据: 配置数据.  每来一条数据, 放入广播状态
                    BroadcastState<String, TableProcess> bdState = ctx.getBroadcastState(tableProcessMapStateDescriptor);
                    // 把配置数据存入的广播状态
                    bdState.put(value.getSourceTable() + ":" + value.getOperateType(), value);
                    
                }
            });
        
    }
    
    // 对数据进行清洗
    private SingleOutputStreamOperator<JSONObject> etlDataStream(DataStreamSource<String> dataStream) {
        
        return dataStream
            .map(JSON::parseObject)
            .filter(obj -> obj.getString("database") != null
                && obj.getString("table") != null
                && obj.getString("type") != null
                && (obj.getString("type").contains("insert") || obj.getString("type").contains("update"))
                && obj.getString("data") != null
                && obj.getString("data").length() > 10);
        
    }
    
    // 读取配置表数据
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 创建一个动态表和配置表进行关联
        tEnv.executeSql("CREATE TABLE `table_process`( " +
                            "   `source_table`  string, " +
                            "   `operate_type`  string, " +
                            "   `sink_type`  string, " +
                            "   `sink_table`  string, " +
                            "   `sink_columns` string, " +
                            "   `sink_pk`  string, " +
                            "   `sink_extend`  string, " +
                            "   PRIMARY KEY (`source_table`,`operate_type`)  NOT ENFORCED" +
                            ")with(" +
                            "   'connector' = 'mysql-cdc', " +
                            "   'hostname' = 'hadoop162', " +
                            "   'port' = '3306', " +
                            "   'username' = 'root', " +
                            "   'password' = 'aaaaaa', " +
                            "   'database-name' = 'gmall2021_realtime', " +
                            "   'table-name' = 'table_process'," +
                            "   'debezium.snapshot.mode' = 'initial' " +  // 每次启动的时候, 首先读取mysql表中的全量数据, 然后在从binlog最新的位置开始读取
                            ")"
        );
        
        Table table = tEnv.sqlQuery("select " +
                                        "source_table sourceTable," +
                                        "sink_type sinkType," +
                                        "operate_type operateType," +
                                        "sink_table sinkTable," +
                                        "sink_columns sinkColumns," +
                                        "sink_pk sinkPk," +
                                        "sink_extend sinkExtend " +
                                        "from table_process");
        return tEnv
            .toRetractStream(table, TableProcess.class)
            .filter(t -> t.f0)
            .map(t -> t.f1);
        
    }
    
}
