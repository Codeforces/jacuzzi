package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** @author Mike Mirzayanov */
public class Jacuzzi {
    /**
     * {@code DataSource} instance,
     * all database routine will use connections
     * created by this instance.
     */
    private final DataSource dataSource;

    /** For each entity class there is correspondent DAO. */
    private final Map<Class<?>, GenericDao<?, ?>> daoCache = new HashMap<Class<?>, GenericDao<?, ?>>();

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
        DataSourceUtil.attachConnection();
    }

    /** Detaches connection from the current thread. */
    public void detachConnection() {
        DataSourceUtil.detachConnection();
    }

    /**
     * Call it to begin transaction around
     * current connection in current thread.
     * <p/>
     * You should call commit() or rollback() at
     * the end of transaction.
     *
     * @param isolationLevel Transaction isolation level
     *                       (use Connection constants).
     */
    public void beginTransaction(int isolationLevel) {
        attachConnection();

        Connection connection = DataSourceUtil.getConnection(dataSource);

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
     * <p/>
     * You should call commit() or rollback() at
     * the end of transaction.
     */
    public void beginTransaction() {
        beginTransaction(Connection.TRANSACTION_SERIALIZABLE);
    }

    /** Commits current transaction. */
    public void commit() {
        Connection connection = DataSourceUtil.getAttachedConnection(dataSource);

        try {
            connection.commit();
            connection.setAutoCommit(true);
            detachConnection();
            DataSourceUtil.closeConnection(connection);
        } catch (SQLException e) {
            throw new DatabaseException("Engine doesn't support transactions or connection is closed.", e);
        }
    }

    /** Rollbacks current transaction. */
    public void rollback() {
        Connection connection = DataSourceUtil.getAttachedConnection(dataSource);

        try {
            connection.rollback();
            connection.setAutoCommit(true);
            detachConnection();
            DataSourceUtil.closeConnection(connection);
        } catch (SQLException e) {
            throw new DatabaseException("Engine doesn't support transactions or connection is closed.");
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
                return PreparedStatementUtil.execute(dataSource, query, args);
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
                return PreparedStatementUtil.findRows(dataSource, query, args);
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
                return PreparedStatementUtil.findFirstRow(dataSource, query, args);
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
                return PreparedStatementUtil.findOne(dataSource, query, args);
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
        return (Date) findOne(query, args);
    }

    /**
     * Returns DAO instance by given DAO class.
     * Gets it from the cache or creates if needed.
     *
     * @param daoClazz of type Class<T> Class<? extends GenericDao<?,?>> instance.
     * @return T DAO instance.
     */
    public <T extends GenericDao<?, ?>> T getDao(Class<T> daoClazz) {
        synchronized (daoCache) {
            if (daoCache.containsKey(daoClazz)) {
                return (T) daoCache.get(daoClazz);
            } else {
                try {
                    Constructor constructor = daoClazz.getDeclaredConstructor(DataSource.class);
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
     * @param instance Instance to be converted.
     * @param clazz Target class.
     * @return T Converted instance.
     */
    public<T> T convertTo(Object instance, Class<T> clazz) {
        return TypeOracle.convertTo(instance, clazz);
    }

    // Contains helper routine.

    /** Thread local cache. */
    private static final ThreadLocal<Map<DataSource, Jacuzzi>> threadCache = new ThreadLocal<Map<DataSource, Jacuzzi>>() {
        /**
         * @return Map<DataSource, Jacuzzi> Creates empty map.
         */
        protected Map<DataSource, Jacuzzi> initialValue() {
            return new HashMap<DataSource, Jacuzzi>();
        }
    };

    /** @return Returns cache map from {@code DataSource} to {@code Jacuzzi}. */
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
        Map<DataSource, Jacuzzi> cache = getThreadCache();

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
}
