package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @ClassName JdbcUtil
 * @Description
 * @Author Lhr
 * @Date 2022/11/23 15:25
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection() {
        String phoenixDriver = Constant.PHOENIX_DRIVER;
        String phoenixUrl = Constant.PHOENIX_URL;
        return getJdbcConnection(phoenixDriver, phoenixUrl, null, null);
    }

    public static Connection getJdbcConnection(String driver, String url, String user, String password) {
        // yarn下不加载会报错
        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("driver驱动类错误=" + driver);
        }
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("连接错误，请检查。url=" + url + " user=" + user + " password=" + password);
        }
    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null && conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
