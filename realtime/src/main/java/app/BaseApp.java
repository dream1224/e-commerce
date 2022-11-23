package app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FlinkSourceUtil;

/**
 * @ClassName BaseApp
 * @Description
 * @Author Lhr
 * @Date 2022/11/21 18:34
 */
public abstract class BaseApp {

    protected abstract void process(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void init(int port, int parallelism, String ckPathAndGroupIdAndJobName, String topic) {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        // 设置服务端口
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        // 设置并行度
        env.setParallelism(parallelism);
        // 设置启用checkpoint
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 设置checkpoint保存地址
        env.getCheckpointConfig().setCheckpointStorage("hdfs://Server1:9820/app/" + ckPathAndGroupIdAndJobName);
        // 设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        // checkpoint并发
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置任务取消时是否保存checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckPathAndGroupIdAndJobName, topic));

        process(env, stream);

        try {
            env.execute(ckPathAndGroupIdAndJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
