package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: Mike Mirzayanov
 */
class DataSourceUtil {
    public static Connection getConnection(DataSource source) throws SQLException {
        return source.getConnection();
    }
}
