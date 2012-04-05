package org.jacuzzi.core;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author Mike Mirzayanov
 */
@SuppressWarnings("AccessOfSystemProperties")
class PreparedStatementUtil {
    private static final Logger logger = Logger.getLogger(PreparedStatementUtil.class);
    
    private static final boolean LOG_SLOW_QUERIES = !"false".equals(System.getProperty("jacuzzi.logSlowQueries"));
    
    private static final String LOG_SLOW_QUERIES_THRESHOLD_STRING
            = System.getProperty("jacuzzi.logSlowQueriesThreshold");
    
    private static final long PRINT_QUERY_TIMES_THRESHOLD = Math.max(
            50L,
            LOG_SLOW_QUERIES_THRESHOLD_STRING == null ? 250L : Long.parseLong(LOG_SLOW_QUERIES_THRESHOLD_STRING)
    );

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static final int MAX_RETRY_COUNT = 10;

    private static ResultSet preparedStatementExecuteQuery(PreparedStatement statement, String query)
            throws SQLException {
        if (LOG_SLOW_QUERIES) {
            long before = System.currentTimeMillis();
            try {
                return statement.executeQuery();
            } finally {
                long duration = System.currentTimeMillis() - before;
                if (duration > PRINT_QUERY_TIMES_THRESHOLD) {
                    logger.warn("Query \"" + query + "\" takes " + duration + " ms.");
                }
            }
        } else {
            return statement.executeQuery();
        }
    }

    private static int preparedQueryExecuteUpdate(PreparedStatement statement, String query) throws SQLException {
        if (LOG_SLOW_QUERIES) {
            long before = System.currentTimeMillis();
            try {
                return statement.executeUpdate();
            } finally {
                long duration = System.currentTimeMillis() - before;
                if (duration > PRINT_QUERY_TIMES_THRESHOLD) {
                    logger.warn("Query \"" + query + "\" takes " + duration + " ms.");
                }
            }
        } else {
            return statement.executeUpdate();
        }
    }

    static List<Row> findRows(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query, final Object... args)
            throws SQLException {
        return runAndReturn(new Invokable<List<Row>>() {
            @Override
            public List<Row> invoke() throws SQLException {
                return internalFindRows(dataSource, dataSourceUtil, query, args);
            }
        });
    }

    private static List<Row> internalFindRows(
            DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object... args) throws SQLException {
        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = preparedStatementExecuteQuery(statement, query);
            statement.clearParameters();

            return Row.readFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    public static int execute(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query,
            final Object[] args, final List<Row> generatedKeys) throws SQLException {
        return runAndReturn(new Invokable<Integer>() {
            @Override
            public Integer invoke() throws SQLException {
                return internalExecute(dataSource, dataSourceUtil, query, args, generatedKeys);
            }
        });
    }

    private static int internalExecute(
            DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args, List<Row> generatedKeys)
            throws SQLException {
        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            try {
                statement = getPreparedStatement(query, connection);
                setupPreparedStatementParameters(statement, args);
                int result = preparedQueryExecuteUpdate(statement, query);
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

    public static Row findFirstRow(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query, final Object[] args)
            throws SQLException {
        return runAndReturn(new Invokable<Row>() {
            @Override
            public Row invoke() throws SQLException {
                return internalFindFirstRow(dataSource, dataSourceUtil, query, args);
            }
        });
    }

    private static Row internalFindFirstRow(
            DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args) throws SQLException {
        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = preparedStatementExecuteQuery(statement, query);
            statement.clearParameters();

            return Row.readFirstFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, connection);
        }
    }

    public static Object findOne(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query, final Object[] args)
            throws SQLException {
        return runAndReturn(new Invokable<Object>() {
            @Override
            public Object invoke() throws SQLException {
                return internalFindOne(dataSource, dataSourceUtil, query, args);
            }
        });
    }

    private static Object internalFindOne(
            DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args) throws SQLException {
        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);
            setupPreparedStatementParameters(statement, args);

            ResultSet resultSet = preparedStatementExecuteQuery(statement, query);

            int columnCount = resultSet.getMetaData().getColumnCount();
            if (columnCount != 1) {
                throw new SQLException("Expected to return exactly one column, but "
                        + columnCount + " found for query " + query);
            }

            Object result = null;
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
                continue;
            }

            if (args[i] instanceof Date) {
                String text = DATE_FORMAT.get().format(args[i]);
                statement.setString(i + 1, text);
                continue;
            }

            if (args[i] instanceof String) {
                statement.setString(i + 1, (String) args[i]);
                continue;
            }

            statement.setObject(i + 1, args[i]);
        }
    }

    private static PreparedStatement getPreparedStatement(String query, Connection connection) throws SQLException {
        return connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    public static <T> T runAndReturn(Invokable<T> invokable) throws SQLException {
        SQLRecoverableException exception = null;

        for (int i = 0; i < MAX_RETRY_COUNT; i++) {
            try {
                return invokable.invoke();
            } catch (SQLRecoverableException e) {
                exception = e;
                try {
                    Thread.sleep(i * i * 100L);
                } catch (InterruptedException ignored) {
                    // No operations.
                }
            }
        }

        throw exception;
    }

    private interface Invokable<T> {
        T invoke() throws SQLException;
    }
}
