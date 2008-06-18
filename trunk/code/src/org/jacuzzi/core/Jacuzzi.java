package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** @author: Mike Mirzayanov */
public class Jacuzzi {
    /**
     * {@code DataSource} instance,
     * all database routine will use connections
     * created by this instance.
     */
    private final DataSource dataSource;

    /**
     * Creates jacuzzi instance by {@code DataSource}.
     *
     * @param dataSource {@code DataSource} instance.
     */
    public Jacuzzi(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Executes query and returns the affected rows count.
     * Use UPDATE, DELETE, INSERT queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The number of affected rows.
     * @throws SQLException In case of can't execute query.
     */
    public int execute(String query, Object... args) throws SQLException {
        synchronized (dataSource) {
            return PreparedStatementUtil.execute(dataSource, query, args);
        }
    }

    /**
     * Executes query and returns selected rows.
     * Use SELECT or SHOW queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return Selected rows.
     * @throws SQLException In case of can't execute query.
     */
    public List<Row> findRows(String query, Object... args) throws SQLException {
        synchronized (dataSource) {
            return PreparedStatementUtil.findRows(dataSource, query, args);
        }
    }

    /**
     * Executes query and returns the first selected row.
     * Use SELECT or SHOW queries here.
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The first selected row.
     * @throws SQLException In case of can't execute query.
     */
    public Row findFirstRow(String query, Object... args) throws SQLException {
        synchronized (dataSource) {
            return PreparedStatementUtil.findFirstRow(dataSource, query, args);
        }
    }

    /**
     * Executes query and returns the only value in the only row.
     * Use something like "SELECT COUNT(*) FROM SomeTable".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row.
     * @throws SQLException In case of can't execute query.
     */
    public Object findOne(String query, Object... args) throws SQLException {
        synchronized (dataSource) {
            return PreparedStatementUtil.findOne(dataSource, query, args);
        }
    }

    /**
     * Executes query and returns the only value in the only row as {@code long}.
     * Use something like "SELECT COUNT(*) FROM SomeTable".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as long.
     * @throws SQLException In case of can't execute query.
     */
    public long findLong(String query, Object... args) throws SQLException {
        return (Long) findOne(query, args);
    }

    /**
     * Executes query and returns the only value in the only row as {@code String}.
     * Use something like "SELECT name FROM SomeTable WHERE id = ?".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as {@code String}.
     * @throws SQLException In case of can't execute query.
     */
    public String findString(String query, Object... args) throws SQLException {
        return (String) findOne(query, args);
    }

    /**
     * Executes query and returns the only value in the only row as {@code Date}.
     * Use something like "SELECT creationTime FROM SomeTable WHERE id = ?".
     *
     * @param query Raw SQL query.
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The only value in the only row as {@code Date}.
     * @throws SQLException In case of can't execute query.
     */
    public Date findDate(String query, Object... args) throws SQLException {
        return (Date) findOne(query, args);
    }

    // Contains helper routine.

    /** Thread local cache. */
    private static final ThreadLocal<Map<DataSource, Jacuzzi>> threadCache = new ThreadLocal<Map<DataSource, Jacuzzi>>() {
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
