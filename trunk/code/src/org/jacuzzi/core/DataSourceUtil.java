package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/** @author Mike Mirzayanov */
class DataSourceUtil {
    /**
     * @param source DataSource instance.
     * @return Connection Connection from the source.
     */
    public static Connection getConnection(DataSource source) {
        try {
            return source.getConnection();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
