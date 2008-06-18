package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

/** @author: Mike Mirzayanov */
class PreparedStatementUtil {
    static List<Row> findRows(DataSource dataSource, String query, Object... args) throws SQLException {
        System.out.println(query);

        Connection connection = DataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = statement.executeQuery();
            statement.clearParameters();

            return Row.readFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            connection.close();
        }
    }

    public static int execute(DataSource dataSource, String query, Object[] args) throws SQLException {
        System.out.println(query);

        Connection connection = DataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            try {
                statement = getPreparedStatement(query, connection);
                setupPreparedStatementParameters(statement, args);
                return statement.executeUpdate();
            } catch (Throwable e) {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                }
                throw new SQLException(e);
            }
        } finally {
            if (!connection.getAutoCommit()) {
                connection.commit();
            }

            tryCloseStatement(statement);
            connection.close();
        }
    }

    public static Row findFirstRow(DataSource dataSource, String query, Object[] args) throws SQLException {
        System.out.println(query);

        Connection connection = DataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = statement.executeQuery();
            statement.clearParameters();

            return Row.readFirstFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            connection.close();
        }
    }

    public static Object findOne(DataSource dataSource, String query, Object[] args) throws SQLException {
        System.out.println(query);

        Connection connection = DataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            Object result = null;

            statement = getPreparedStatement(query, connection);
            setupPreparedStatementParameters(statement, args);

            ResultSet resultSet = statement.executeQuery();

            int columnCount = resultSet.getMetaData().getColumnCount();
            if (columnCount != 1) {
                throw new SQLException("Expected to return exactly one column, but "
                        + columnCount + " found for query " + query);
            }

            if (resultSet.next()) {
                result = resultSet.getObject(1);
            }

            if (resultSet.next()) {
                throw new SQLException("Expected to return exactly one row by "
                        + query);
            }

            resultSet.close();

            statement.clearParameters();

            return result;
        } finally {
            tryCloseStatement(statement);
            connection.close();
        }
    }

    private static void tryCloseStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void setupPreparedStatementParameters(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            statement.setObject(i + 1, args[i]);
        }
    }

    private static PreparedStatement getPreparedStatement(String query, Connection connection) throws SQLException {
        return connection.prepareStatement(query);
    }
}
