package app;


import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Constant;
import util.FlinkSinkUtil;
import util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;


/**
 * @ClassName Dim
 * @Description
 * @Author Lhr
 * @Date 2022/11/21 15:39
 */
public class Dim extends BaseApp {
    private static final Logger logger = LoggerFactory.getLogger(Dim.class.getSimpleName());

    public static void main(String[] args) {
        new Dim().init(2000, 2, "Dim", Constant.TOPIC_ODS_DB);

    }

    @Override
    protected void process(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 对流进行操作
        // 1.对业务数据etl过滤
        DataStream<JSONObject> etlStream = etl(stream);

        // 2.读取配置信息
        DataStream<TableProcess> tpStream = tableProcessSource(env);

        // 3.数据流和广播流connect
        DataStream<Tuple2<JSONObject, TableProcess>> connectStream = connectStream(etlStream, tpStream);

        // 4.保留只需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = removeColumns(connectStream);

        // 5.根据不同的配置信息，将不同的维度写入不同的Phoenix表中
        writeToPhoenix(resultStream);

    }

    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> streamOperator) {
        streamOperator.addSink(FlinkSinkUtil.getPhoenixSink());

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> removeColumns(DataStream<Tuple2<JSONObject, TableProcess>> connectStream) {
        return connectStream.map((MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>) tuple2 -> {
            JSONObject jsonObject = tuple2.f0;
            List<String> columnList = Arrays.asList(tuple2.f1.getSinkColumns().split(","));
            jsonObject.keySet().removeIf(key -> !columnList.contains(key) && !"op_type".equals(key));
            return new Tuple2<>(jsonObject,tuple2.f1);
        });
    }

    private DataStream<Tuple2<JSONObject, TableProcess>> connectStream(DataStream<JSONObject> dataStream, DataStream<TableProcess> tpStream) {
        // 根据配置信息在Phoenix中创建相应的维度表，建表语句应该在广播之前，否则会出现多次建表
        tpStream = tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {
            private Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 避免与服务器的长链接，长时间没有使用，服务器会字段关闭连接 或者使用连接池
                conn = JdbcUtil.getPhoenixConnection();

            }

            @Override
            public void close() throws Exception {
                JdbcUtil.closeConnection(conn);
            }

            @Override
            public TableProcess map(TableProcess tableProcess) throws Exception {
                // create table if not exist table()
                // 拼接建表sql
                if (conn.isClosed()){
                    conn = JdbcUtil.getPhoenixConnection();
                }

                StringBuilder sql = new StringBuilder();

                sql
                        .append("create table if not exist")
                        .append(tableProcess.getSinkTable())
                        .append("(")
                        .append(tableProcess.getSinkColumns().replaceAll("[^,]+", "$1 varchar"))
                        .append(", constraint pk primary key")
                        .append(tableProcess.getSinkPk() == null ? "pk" : tableProcess.getSinkPk())
                        // 盐表预分区
                        .append(tableProcess.getSinkExtend() == null ? "" : tableProcess.getSinkExtend())
                        .append(")");

                // 获取预处理语句
                PreparedStatement ps = conn.prepareStatement(sql.toString());

                // 给sql中的占位符赋值(增删改查)，ddl没有占位符

                // 执行
                ps.execute();

                // 关闭
                ps.close();

                return tableProcess;
            }
        });


        // 配置流做成广播流
        // key=sourceTable
        // value=TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = tpStream.broadcast(tpStateDesc);
        // 数据流connect广播流
        return dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    // 处理数据流中的元素
                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        // 处理数据流中的数据时，从广播状态读取对应的配置信息
                        // 根据mysql表名获取配置信息
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(tpStateDesc);
                        String table = jsonObject.getString("table");
                        if (table != null) {
                            TableProcess tableProcess = broadcastState.get(table);
                            // 只保留数据中的有用信息
                            JSONObject data = jsonObject.getJSONObject("data");
                            // 操作类型保留到data
                            data.put("op_type",jsonObject.getString("type"));
                            collector.collect(Tuple2.of(data, tableProcess));
                        }
                    }

                    // 处理广播流中的元素
                    @Override
                    public void processBroadcastElement(TableProcess tableProcess, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context context, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        // 配置信息写入广播状态
                        String key = tableProcess.getSourceTable();
                        BroadcastState<String, TableProcess> state = context.getBroadcastState(tpStateDesc);
                        state.put(key, tableProcess);
                    }
                });
    }

    private DataStream<TableProcess> tableProcessSource(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("Server1")
                .port(3306)
                // set captured database
                .databaseList("gmall_config")
                // set captured table
                .tableList("gmall_config.table-process")
                .username("root")
                .password("123456")
                // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                // INITIAL,
                // EARLIEST_OFFSET,
                // LATEST_OFFSET,
                // SPECIFIC_OFFSETS,
                // TIMESTAMP
                .startupOptions(StartupOptions.initial())
                .build();
        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    // 会自动解析成bean类的字段格式
                    return obj.getObject("after", TableProcess.class);
                });

    }

    private DataStream<JSONObject> etl(DataStreamSource<String> stream) {
        // 过滤json结构数据，以及指定数据库和操作类型的数据
        return stream
                .filter(json -> {
                    try {

                        JSONObject obj = JSON.parseObject(json);
                        return ("bootstrap-insert".equals(obj.getString("type")) || "bootstrap-update".equals(obj.getString("type")))
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 2;
                    } catch (Exception e) {
                        logger.warn("json格式有误，json=" + json);
                        e.printStackTrace();
                        return false;
                    }
                })
                .map(JSON::parseObject);
    }
}
