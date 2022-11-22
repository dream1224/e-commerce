package util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName FlinkSourceUtil
 * @Description
 * @Author Lhr
 * @Date 2022/11/21 19:03
 */
public class FlinkSourceUtil {

    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        // 开启了2阶段提交时，数据分为已提交和未提交的数据，这里需要消费已提交的数据
        props.setProperty("isolation.level", "read_committed");
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }
}
