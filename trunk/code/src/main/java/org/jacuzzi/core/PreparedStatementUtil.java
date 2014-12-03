package org.jacuzzi.core;

import org.apache.commons.lang.ArrayUtils;
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
    private static final boolean DEBUG_QUERIES = "true".equals(System.getProperty("jacuzzi.debugQueries"));

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

    private static ResultSet preparedStatementExecuteQuery(PreparedStatement statement, String query, Object[] args)
            throws SQLException {
        if (LOG_SLOW_QUERIES || DEBUG_QUERIES) {
            long before = System.currentTimeMillis();
            try {
                return statement.executeQuery();
            } finally {
                long duration = System.currentTimeMillis() - before;
                if (duration > PRINT_QUERY_TIMES_THRESHOLD) {
                    logger.warn(String.format(
                            "Query \"%s\" with parameters [%s] takes %d ms.", query, formatParameters(args), duration
                    ));
                }
                if (DEBUG_QUERIES) {
                    logger.debug(   String.format(
                            "Query \"%s\" with parameters [%s] takes %d ms.", query, formatParameters(args), duration
                    ));
                }
            }
        } else {
            return statement.executeQuery();
        }
    }

    private static int preparedQueryExecuteUpdate(PreparedStatement statement, String query, Object[] args)
            throws SQLException {
        if (LOG_SLOW_QUERIES || DEBUG_QUERIES) {
            long before = System.currentTimeMillis();
            try {
                return statement.executeUpdate();
            } finally {
                long duration = System.currentTimeMillis() - before;
                if (LOG_SLOW_QUERIES && duration > PRINT_QUERY_TIMES_THRESHOLD) {
                    logger.warn(String.format(
                            "Query \"%s\" with parameters [%s] takes %d ms.", query, formatParameters(args), duration
                    ));
                }
                if (DEBUG_QUERIES) {
                    logger.debug(String.format(
                            "Query \"%s\" with parameters [%s] takes %d ms.", query, formatParameters(args), duration
                    ));
                }
            }
        } else {
            return statement.executeUpdate();
        }
    }

    private static String formatParameters(Object[] args) {
        StringBuilder builder = new StringBuilder();

        for (int argIndex = 0, argCount = args.length; argIndex < argCount; ++argIndex) {
            if (argIndex > 0) {
                builder.append(", ");
            }

            Object argument = args[argIndex];
            if (argument == null) {
                builder.append("null");
            } else if (argument.getClass().isArray()) {
                builder.append(ArrayUtils.toString(argument));
            } else {
                builder.append('\'').append(argument.toString()).append('\'');
            }
        }

        return builder.toString();
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
            ResultSet resultSet = preparedStatementExecuteQuery(statement, query, args);
            statement.clearParameters();

            return Row.readFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, dataSource, connection);
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
                int result = preparedQueryExecuteUpdate(statement, query, args);
                if (generatedKeys != null) {
                    generatedKeys.addAll(Row.readFromResultSet(statement.getGeneratedKeys()));
                }
                return result;
            } catch (Throwable e) {
                throw new SQLException(e);
            }
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, dataSource, connection);
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
            ResultSet resultSet = preparedStatementExecuteQuery(statement, query, args);
            statement.clearParameters();

            return Row.readFirstFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, dataSource, connection);
        }
    }

    private static Object wrapResult(Object result) {
        if (result == null) {
            return result;
        }

        if (result.getClass().equals(Timestamp.class)) {
            result = new Date(((java.sql.Timestamp) result).getTime());
        }

        if (result.getClass().equals(java.sql.Date.class)) {
            result = new Date(((java.sql.Date) result).getTime());
        }

        return result;
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

            ResultSet resultSet = preparedStatementExecuteQuery(statement, query, args);

            int columnCount = resultSet.getMetaData().getColumnCount();
            if (columnCount != 1) {
                throw new SQLException(String.format(
                        "Expected to return exactly one column, but %d found for query %s", columnCount, query
                ));
            }

            Object result = null;
            if (resultSet.next()) {
                result = resultSet.getObject(1);
            }

            if (resultSet.next()) {
                throw new SQLException(String.format("Expected to return exactly one row by query %s", query));
            }

            resultSet.close();
            statement.clearParameters();

            return wrapResult(result);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, dataSource, connection);
        }
    }

    private static void tryCloseConnection(DataSourceUtil dataSourceUtil, DataSource dataSource, Connection connection) {
        dataSourceUtil.closeConnection(dataSource, connection);
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

        for (int i = 0; i < MAX_RETRY_COUNT; ++i) {
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
