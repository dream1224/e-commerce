package app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.FlinkSourceUtil;

/**
 * @ClassName Dim
 * @Description
 * @Author Lhr
 * @Date 2022/11/21 15:39
 */
public class Dim {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // 设置服务端口
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置并行度
        env.setParallelism(2);
        // 设置启用checkpoint
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 设置checkpoint保存地址
        env.getCheckpointConfig().setCheckpointStorage("hdfs://Server1:8020/app/Dim");
        // 设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        // checkpoint并发
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置任务取消时是否保存checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource("Dim", "ods_db"));
        stream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
