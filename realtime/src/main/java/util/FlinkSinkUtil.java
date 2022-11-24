package util;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sink.PhoenixSink;

/**
 * @ClassName FlinkSinkUtil
 * @Description
 * @Author Lhr
 * @Date 2022/11/23 20:00
 */
public class FlinkSinkUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
