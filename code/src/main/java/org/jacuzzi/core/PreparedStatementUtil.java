package org.jacuzzi.core;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.math.BigInteger;
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

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    private static final int MAX_RETRY_COUNT = 10;

    private static ResultSet preparedStatementExecuteQuery(PreparedStatement statement, String query, Object[] args)
            throws SQLException {
        ResultSet result;
        long before = System.currentTimeMillis();
        long duration;
        try {
            result = statement.executeQuery();
        } finally {
            duration = System.currentTimeMillis() - before;
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
        QueryPostHandlerUtil.handle(new QueryPostHandler.Query(query, args, statement, duration));
        return result;
    }

    private static int preparedQueryExecuteUpdate(PreparedStatement statement, String query, Object[] args)
            throws SQLException {
        int result = 0;
        long before = System.currentTimeMillis();
        long duration;
        try {
            result = statement.executeUpdate();
        } finally {
            duration = System.currentTimeMillis() - before;
            if (LOG_SLOW_QUERIES && duration > PRINT_QUERY_TIMES_THRESHOLD) {
                logger.warn(String.format(
                        "Query \"%s\" with parameters [%s] takes %d ms, updated %d rows.", query, formatParameters(args), duration, result
                ));
            }
            if (DEBUG_QUERIES) {
                logger.debug(String.format(
                        "Query \"%s\" with parameters [%s] takes %d ms, updated %d rows.", query, formatParameters(args), duration, result
                ));
            }
        }
        QueryPostHandlerUtil.handle(new QueryPostHandler.Query(query, args, statement, duration));
        return result;
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
        return runAndReturn(() -> internalFindRows(dataSource, dataSourceUtil, query, args));
    }

    static RowRoll findRowRoll(DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args)
            throws SQLException {
        return runAndReturn(() -> internalFindRowRoll(dataSource, dataSourceUtil, query, args));
    }

    private static RowRoll internalFindRowRoll(DataSource dataSource, DataSourceUtil dataSourceUtil, String query, Object[] args)
            throws SQLException {
        Connection connection = dataSourceUtil.getConnection(dataSource);
        PreparedStatement statement = null;

        try {
            statement = getPreparedStatement(query, connection);

            setupPreparedStatementParameters(statement, args);
            ResultSet resultSet = preparedStatementExecuteQuery(statement, query, args);
            statement.clearParameters();

            return Row.readRowRollFromResultSet(resultSet);
        } finally {
            tryCloseStatement(statement);
            tryCloseConnection(dataSourceUtil, dataSource, connection);
        }
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

    static int execute(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query,
            final Object[] args, final List<Row> generatedKeys) throws SQLException {
        return runAndReturn(() -> internalExecute(dataSource, dataSourceUtil, query, args, generatedKeys));
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

    static Row findFirstRow(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query, final Object[] args)
            throws SQLException {
        return runAndReturn(() -> internalFindFirstRow(dataSource, dataSourceUtil, query, args));
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
            return null;
        }

        if (result.getClass().equals(Timestamp.class)) {
            result = new Date(((java.sql.Timestamp) result).getTime());
        }

        if (result.getClass().equals(java.sql.Date.class)) {
            result = new Date(((java.sql.Date) result).getTime());
        }

        return result;
    }

    @SuppressWarnings("WeakerAccess")
    public static Object findOne(
            final DataSource dataSource, final DataSourceUtil dataSourceUtil, final String query, final Object[] args)
            throws SQLException {
        return runAndReturn(() -> internalFindOne(dataSource, dataSourceUtil, query, args));
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
                result = prepareResultSetGetObject(resultSet.getObject(1));
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
        for (int i = 0; i < args.length; ++i) {
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

    private static <T> T runAndReturn(Invokable<T> invokable) throws SQLException {
        SQLException exception = null;

        for (int i = 0; i < MAX_RETRY_COUNT; ++i) {
            try {
                return invokable.invoke();
            } catch (SQLRecoverableException | SQLNonTransientConnectionException e) {
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

    static Object prepareResultSetGetObject(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) object;
            try {
                object = bigInteger.longValueExact();
            } catch (ArithmeticException ignored) {
                // No operations.
            }
        } else if (object instanceof Blob) {
            Blob blob = (Blob) object;
            try {
                object = blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new DatabaseException("Can't read blob.", e);
            }
        } else if (object instanceof Clob) {
            Clob clob = (Clob) object;
            try {
                object = clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new DatabaseException("Can't read clob.", e);
            }
        }

        return object;
    }

    private interface Invokable<T> {
        T invoke() throws SQLException;
    }
}
