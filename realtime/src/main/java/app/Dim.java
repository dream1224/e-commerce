package app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Constant;


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
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);

        // 2.修改配置信息

        // 3.数据流和广播流connect

        // 4.根据不同的配置信息，将不同的维度写入不同的Phoenix表中


    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        // 过滤json结构数据，以及指定数据库和操作类型的数据
        return stream
                .filter(json -> {
                    try {
                        JSONObject obj = JSON.parseObject(json);
                        return ("insert".equals(obj.getString("type")) || "update".equals(obj.getString("type")))
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 2;
                    } catch (Exception e) {
                        logger.warn("json格式有误，json=" + json);
                        e.printStackTrace();
                        return false;
                    }
                })
                .map(JSON::parseObject)
        ;
    }
}
