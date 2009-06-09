package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

/** @author Mike Mirzayanov */
class PreparedStatementUtil {
    static List<Row> findRows(DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object... args) throws SQLException {
        //System.out.println(query);

        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = statement.executeQuery();
            statement.clearParameters();

            return Row.readFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    public static int execute(DataSource dataSource, DataSourceUtil dataSourceUtil, String query,
                              Object[] args, List<Row> generatedKeys) throws SQLException {
        //System.out.println(query);

        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            try {
                statement = getPreparedStatement(query, connection);
                setupPreparedStatementParameters(statement, args);
                int result = statement.executeUpdate();
                if (generatedKeys != null) {
                    generatedKeys.addAll(Row.readFromResultSet(statement.getGeneratedKeys()));
                }
                return result;
            } catch (Throwable e) {
                throw new SQLException(e);
            }
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    public static Row findFirstRow(DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args) throws SQLException {
        //System.out.println(query);

        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = statement.executeQuery();
            statement.clearParameters();

            return Row.readFirstFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    public static Object findOne(DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args) throws SQLException {
        //System.out.println(query);

        Connection connection = dataSourceUtil.getConnection(dataSource);
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
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    private static void tryCloseConnection(DataSourceUtil dataSourceUtil, Connection connection) {
        dataSourceUtil.closeConnection(connection);
    }

    private static void tryCloseStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                throw new DatabaseException("Can't close statement.", e);
            }
        }
    }

    private static void setupPreparedStatementParameters(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof Enum) {
                statement.setString(i + 1, args[i].toString());
            } else {
                statement.setObject(i + 1, args[i]);
            }
        }
    }

    private static PreparedStatement getPreparedStatement(String query, Connection connection) throws SQLException {
        return connection.prepareStatement(query);
    }
}
