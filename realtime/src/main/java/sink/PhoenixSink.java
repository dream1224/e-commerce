package sink;

import bean.TableProcess;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import util.DruidDSUtil;
import util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @ClassName PhoenixSink
 * @Description
 * @Author Lhr
 * @Date 2022/11/23 20:01
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取连接池
        dataSource = DruidDSUtil.getDataSource();
    }

    @Override
    public void close() throws Exception {
        // app关闭之前关闭连接池
        dataSource.close();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 每来一条数据从连接池获取一个可用连接，这样可以避免长连接被服务器自动关闭的问题
        DruidPooledConnection conn = dataSource.getConnection();

        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        // 拼接sql语句，为了避免sql注入，拼接占位符
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(") values")
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");
        // 获取prepareStatement
        PreparedStatement ps = conn.prepareStatement(sql.toString());

        // 给占位符赋值
        String[] columns = tp.getSinkColumns().split(",");
        for (int i = 0; i < columns.length; i++) {
            Object obj = data.get(columns[i]);
            String result = obj == null ? null : obj.toString();
            ps.setString(i + 1, result);
        }

        // 执行sql
        ps.execute(sql.toString());

        // phoenix写时不会自动提交
        conn.commit();

        // 关闭prepareStatement
        ps.close();

        // 归还连接
        conn.close();

    }
}
