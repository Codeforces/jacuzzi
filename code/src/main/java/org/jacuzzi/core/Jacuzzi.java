package org.jacuzzi.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * @author Mike Mirzayanov
 */
public class Jacuzzi {
    private static final Logger logger = Logger.getLogger(Jacuzzi.class);

    public static final ZoneOffset ZONE_OFFSET;

    /**
     * {@code DataSource} instance,
     * all database routine will use connections
     * created by this instance.
     */
    private final DataSource dataSource;

    /**
     * Helper routine around {@code DataSource}.
     */
    private final DataSourceUtil dataSourceUtil = new DataSourceUtil();

    /**
     * For each entity class there is correspondent DAO.
     */
    private final Map<Class<?>, GenericDao<?, ?>> daoCache = new HashMap<>();

    /**
     * Creates jacuzzi instance by {@code DataSource}.
     *
     * @param dataSource {@code DataSource} instance.
     */
    protected Jacuzzi(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Attaches connection to the current thread.
     * Starting from the call Jacuzzi will operate with
     * single JDBC connection. Use detachConnection() to
     * detach it.
     */
    public void attachConnection() {
        dataSourceUtil.attachConnection(dataSource);
    }

    /**
     * Detaches connection from the current thread.
     */
    public void detachConnection() {
        dataSourceUtil.detachConnection(dataSource);
    }

    /**
     * Detaches connection from the current thread.
     */
    public void detachConnectionOrThrowException() {
        dataSourceUtil.detachConnectionOrThrowException(dataSource);
    }

    /**
     * Call it to begin transaction around
     * current connection in current thread.
     *
     * You should call commit() or rollback() at
     * the end of transaction.
     *
     * @param isolationLevel Transaction isolation level
     *                       (use Connection constants).
     */
    public void beginTransaction(int isolationLevel) {
        attachConnection();
        Connection connection = dataSourceUtil.getConnection(dataSource);

        try {
            connection.setTransactionIsolation(isolationLevel);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DatabaseException("Engine doesn't support transactions or connection is closed.", e);
        }
    }

    /**
     * Call it to begin transaction around
     * current connection in current thread.
     *
     * You should call commit() or rollback() at
     * the end of transaction.
     */
    public void beginTransaction() {
        beginTransaction(Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * Commits current transaction.
     */
    public void commit() {
        Connection connection = dataSourceUtil.getAttachedConnection(dataSource);

        try {
            connection.commit();
        } catch (SQLException e) {
            throw new DatabaseException("Engine doesn't support transactions or connection is closed.", e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException ignored) {
                // No operations.
            }
            detachConnection();
        }
    }

    /**
     * Rollbacks current transaction.
     */
    public void rollback() {
        Connection connection = dataSourceUtil.getAttachedConnection(dataSource);

        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new DatabaseException("Engine doesn't support transactions or connection is closed.", e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException ignored) {
                // No operations.
            }
            detachConnection();
        }
    }

    /**
     * Executes query and returns the affected rows count.
     * Use UPDATE, DELETE, INSERT queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The number of affected rows.
     */
    public int execute(String query, Object... args) {
        try {
            return PreparedStatementUtil.execute(dataSource, dataSourceUtil, query, args, null);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes INSERT query and returns InsertResult which
     * contains count and generated keys (auto-increments).
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return InsertResult which
     *         contains count and generated keys (auto-increments).
     */
    public InsertResult insert(String query, Object... args) {
        try {
            List<Row> generated = new ArrayList<>(1);
            int count = PreparedStatementUtil.execute(dataSource, dataSourceUtil, query, args, generated);
            if (generated.isEmpty()) {
                List<Row> generatedKeys = new ArrayList<>(1);
                generatedKeys.add(new Row(1));
                return new InsertResult(count, generatedKeys);
            } else {
                if (generated.size() > 1) {
                    throw new DatabaseException("Unexpected generated key size " + generated.size() + '.');
                }
                return new InsertResult(count, generated);
            }
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes INSERT query for multiple rows and returns
     * InsertResult which contains count and
     * generated keys (auto-increments).
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return InsertResult which
     *         contains count and generated keys (auto-increments).
     */
    public InsertResult multipleInsert(String query, Object... args) {
        try {
            List<Row> generated = new ArrayList<>();
            int count = PreparedStatementUtil.execute(dataSource, dataSourceUtil, query, args, generated);

            return new InsertResult(count, generated);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes query and returns selected rows.
     * Use SELECT or SHOW queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return Selected rows.
     */
    public List<Row> findRows(String query, Object... args) {
        try {
            return PreparedStatementUtil.findRows(dataSource, dataSourceUtil, query, args);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes query and returns selected rows.
     * Use SELECT or SHOW queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return Selected rows as RowRoll.
     */
    public RowRoll findRowRoll(String query, Object... args) {
        try {
            return PreparedStatementUtil.findRowRoll(dataSource, dataSourceUtil, query, args);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes query and returns the first selected row.
     * Use SELECT or SHOW queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The first selected row.
     */
    public Row findFirstRow(String query, Object... args) {
        try {
            return PreparedStatementUtil.findFirstRow(dataSource, dataSourceUtil, query, args);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes query and returns the only value in the only row.
     * Use something like "SELECT COUNT(*) FROM SomeTable".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row.
     */
    public Object findOne(String query, Object... args) {
        try {
            return PreparedStatementUtil.findOne(dataSource, dataSourceUtil, query, args);
        } catch (SQLException e) {
            System.err.println(query);
            throw new DatabaseException(e);
        }
    }

    /**
     * Executes query and returns the only value in the only row as {@code long}.
     * Use something like "SELECT COUNT(*) FROM SomeTable".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as long.
     */
    public long findLong(String query, Object... args) {
        return (Long) findOne(query, args);
    }

    /**
     * Executes query and returns the only value in the only row as {@code boolean}.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as long.
     */
    public boolean findBoolean(String query, Object... args) {
        return (Boolean) findOne(query, args);
    }

    /**
     * Executes query and returns the only value in the only row as {@code String}.
     * Use something like "SELECT name FROM SomeTable WHERE id = ?".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as {@code String}.
     */
    public String findString(String query, Object... args) {
        return (String) findOne(query, args);
    }

    /**
     * Executes query and returns the only value in the only row as {@code Date}.
     * Use something like "SELECT creationTime FROM SomeTable WHERE id = ?".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as {@code Date}.
     */
    public Date findDate(String query, Object... args) {
        Object result = findOne(query, args);
        if (result == null) {
            return null;
        }
        if (result instanceof Date) {
            return (Date) result;
        }
        if (result instanceof OffsetDateTime) {
            long epochMilli = ((OffsetDateTime) result).toInstant().toEpochMilli();
            return new Date(epochMilli);
        }
        if (result instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) result).toInstant(ZONE_OFFSET));
        }
        throw new DatabaseException("Query `" + query + "` expected to return Date, OffsetDateTime or LocalDateTime.");
    }

    /**
     * Returns DAO instance by given DAO class.
     * Gets it from the cache or creates if needed.
     *
     * @param <T> Entity type.
     * @param daoClazz Instance class.
     * @return T DAO instance.
     */
    @SuppressWarnings({"unchecked"})
    public <T extends GenericDao<?, ?>> T getDao(Class<T> daoClazz) {
        synchronized (daoCache) {
            if (daoCache.containsKey(daoClazz)) {
                return (T) daoCache.get(daoClazz);
            } else {
                try {
                    Constructor<?> constructor = daoClazz.getDeclaredConstructor(DataSource.class);
                    constructor.setAccessible(true);
                    GenericDao<?, ?> dao = (GenericDao<?, ?>) constructor.newInstance(dataSource);
                    daoCache.put(daoClazz, dao);
                    return (T) dao;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    /**
     * This method you can use to convert the data from JDBC into specific
     * java instances. For example, JDBC returns string but we need enum instance.
     *
     * @param <T> Type convert to.
     * @param instance Instance to be converted.
     * @param clazz    Target class.
     * @return T Converted instance.
     */
    public <T> T convertTo(Object instance, Class<T> clazz) {
        return TypeOracle.convertTo(instance, clazz);
    }

    // Contains helper routine.

    /**
     * Thread local cache.
     */
    private static final ThreadLocal<Map<DataSource, Jacuzzi>> threadCache = new ThreadLocal<Map<DataSource, Jacuzzi>>() {
        /**
         * @return {@code Map&lt;DataSource, Jacuzzi&gt;} Creates empty map.
         */
        @Override
        protected Map<DataSource, Jacuzzi> initialValue() {
            return new HashMap<>();
        }
    };

    /**
     * @return Returns cache map from {@code DataSource} to {@code Jacuzzi}.
     */
    private static Map<DataSource, Jacuzzi> getThreadCache() {
        return threadCache.get();
    }

    /**
     * Factory method to create {@code Jacuzzi} by {@code DataSource}.
     * It gets created before instances from cache.
     *
     * @param source {@code DataSource} instance.
     * @return {@code Jacuzzi} instance.
     */
    public static Jacuzzi getJacuzzi(DataSource source) {
        final Map<DataSource, Jacuzzi> cache = getThreadCache();

        synchronized (cache) {
            if (cache.containsKey(source)) {
                return cache.get(source);
            } else {
                Jacuzzi jacuzzi = new Jacuzzi(source);
                cache.put(source, jacuzzi);
                return jacuzzi;
            }
        }
    }

    public static class InsertResult {
        private int count;
        private final List<Row> generatedKeys;

        private InsertResult(int count, List<Row> generatedKeys) {
            this.count = count;
            this.generatedKeys = generatedKeys;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public Row getGeneratedKeysForRow(int rowIndex) {
            return generatedKeys.get(rowIndex);
        }

        public Row getGeneratedKeysForOneRow() {
            return generatedKeys.get(0);
        }

        public List<Row> getGeneratedKeys() {
            return Collections.unmodifiableList(generatedKeys);
        }
    }

    static {
        String zoneOffsetId = System.getProperty("jacuzzi.zoneOffsetId");
        if (StringUtils.isBlank(zoneOffsetId)) {
            logger.warn("No JVM option -Djacuzzi.zoneOffsetId, use +3.");
            zoneOffsetId = "+3";
        }
        logger.info("Property jacuzzi.zoneOffsetId='" + zoneOffsetId + "'.");
        ZONE_OFFSET = ZoneOffset.of(zoneOffsetId);
    }
}
