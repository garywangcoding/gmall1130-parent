package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/22 9:18
 */
public class DWDDbApp extends BaseApp {
    public static void main(String[] args) {
        new DWDDbApp().init(2002, 2, "DWDDbApp", "DWDDbApp", Constant.ODS_DB);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> dataStream) {
        // 1. 对业务数据进行etl清洗
        SingleOutputStreamOperator<JSONObject> etledDataStream = etlDataStream(dataStream);
        // 2. 读取配置表的内容(cdc)
        SingleOutputStreamOperator<TableProcess> tableProcessStream = readTableProcess(env);
        
        // 3. 把配置表做成广播流, 与数据进行connect, 利用广播状态来控制每个数据流的流向
        dynamicSplitStream(etledDataStream, tableProcessStream);
    }
    
    // 动态分流
    private void dynamicSplitStream(SingleOutputStreamOperator<JSONObject> etledDataStream,
                                    SingleOutputStreamOperator<TableProcess> tableProcessStream) {
        MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor =
            new MapStateDescriptor<>("tableProcessState", String.class, TableProcess.class);
        
        // Key的类型: "order_info:insert"   value的类型:
        BroadcastStream<TableProcess> tableProcessBCStream = tableProcessStream
            .broadcast(tableProcessMapStateDescriptor);
    
        etledDataStream
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
                    out.collect(Tuple2.of(value, tableProcess));
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
            })
            .print();
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
