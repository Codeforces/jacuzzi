package example;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import example.model.User;
import example.dao.UserDao;
import org.jacuzzi.core.Row;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Mike Mirzayanov
 */
public class Main {
    public static String URL = "jdbc:mysql://localhost:3307/velocity";
    public static String USER = "root";
    public static String PASSWORD = "tco99708";

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        Class.forName("com.mysql.jdbc.Driver");
        DataSource source = createC3p0();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            Test.main(source);
        }
        System.out.println((System.currentTimeMillis() - start) + " ms.");
    }

    private static void checkSource(DataSource source) throws SQLException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            Connection connection = source.getConnection();

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SHOW TABLES");

            System.err.println("Case " + i);
            show(resultSet);
            Thread.sleep(1000);

            connection.close();
        }
    }

    private static DataSource createC3p0() throws SQLException {
//        DataSource source = DataSources.unpooledDataSource(URL, USER, PASSWORD);
//        PoolConfig config = new PoolConfig();
//        //config.setCheckoutTimeout(Integer.MAX_VALUE);
//        return DataSources.pooledDataSource(source, config);

        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl(URL);
        comboPooledDataSource.setUser(USER);
        comboPooledDataSource.setPassword(PASSWORD);
        comboPooledDataSource.setCheckoutTimeout(60000);
        comboPooledDataSource.setIdleConnectionTestPeriod(5);
        comboPooledDataSource.setMaxStatementsPerConnection(32);
        comboPooledDataSource.setPreferredTestQuery("SELECT NOW()");
        return comboPooledDataSource;
    }

    private static MysqlDataSource createMysql() throws SQLException {
        MysqlDataSource source = //new MysqlDataSource();
                new MysqlConnectionPoolDataSource();

        source.setMaxReconnects(1000);
        source.setAutoReconnect(true);
        source.setLoginTimeout(Integer.MAX_VALUE);
        source.setTcpKeepAlive(true);
        source.setLogWriter(new PrintWriter(System.err));

        source.setURL(URL);
        source.setUser(USER);
        source.setPassword(PASSWORD);
        return source;
    }

    private static void show(ResultSet resultSet) throws SQLException {
        List<Row> result = new LinkedList<Row>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        while (resultSet.next()) {
            Row row = new Row();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row.put(metaData.getColumnName(i), resultSet.getObject(i));
                //System.err.println(metaData.getColumnName(i) + ": " + resultSet.getObject(i));
            }
            //System.err.println("\n");
            result.add(row);
        }

        System.err.println(result.size());

        resultSet.close();
    }
}
