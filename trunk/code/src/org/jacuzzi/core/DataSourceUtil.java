package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;

/** @author Mike Mirzayanov */
class DataSourceUtil {
    /**
     * Connections by threads.
     * It used in attachConnection()/detachConnection() functionality.
     */
    private static final Map<Thread, Connection> connectionsByThread
            = new HashMap<Thread, Connection>();

    /**
     * Leases for attached connections.
     * If lease exceed with value, exception will be thrown.
     */
    private static final Map<Thread, Long> leasesByThread
            = new HashMap<Thread, Long>();

    /**
     * Time in milliseconds for lease.
     */
    private static final int LEASE_TIMEOUT = 30000;

    /**
     * Attaches connection to the current thread.
     * <p/>
     * Use it if you want several SQL statements be executed
     * by the same connection.
     * <p/>
     * You should call detachConnection() after you don't need
     * this feature.
     * <p/>
     * If it takes too long before detachConnection(), system
     * assumes that you forgot to call it and raises DatabaseException.
     */
    public static synchronized void attachConnection() {
        Thread currentThread = Thread.currentThread();

        if (connectionsByThread.containsKey(currentThread)) {
            throw new DatabaseException("It seems you've didn't call detachConnection() " +
                    "before you've called attachConnection().");
        }

        connectionsByThread.put(currentThread, null);
        leasesByThread.put(currentThread, System.currentTimeMillis() + LEASE_TIMEOUT);
    }

    /**
     * Call it (usually in finally section) when you want to
     * detach connection from the thread.
     */
    public static synchronized void detachConnection() {
        Thread currentThread = Thread.currentThread();

        connectionsByThread.remove(currentThread);
        leasesByThread.remove(currentThread);
    }

    /**
     * @param source DataSource instance.
     * @return Connection Connection from the source.
     */
    public static synchronized Connection getConnection(DataSource source) {
        Thread currentThread = Thread.currentThread();

        if (connectionsByThread.containsKey(currentThread)) {
            long leaseTime = leasesByThread.get(currentThread);

            if (leaseTime > System.currentTimeMillis()) {
                throw new DatabaseException("It seems you've didn't call detachConnection().");
            }

            Connection connection = connectionsByThread.get(currentThread);
            if (connection != null) {
                return connection;
            }
        }

        try {
            Connection connection = source.getConnection();

            if (connectionsByThread.containsKey(currentThread)) {
                connectionsByThread.put(currentThread, connection);
            }

            return connection;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Similar to getConnection(), but it checks that the connection is attached.
     *
     * @param source DataSource instance.
     * @return Connection Connection from the source.
     */
    public static synchronized Connection getAttachedConnection(DataSource source) {
        if (!connectionsByThread.containsKey(Thread.currentThread())) {
            throw new DatabaseException("Connection is not attached.");
        }

        return getConnection(source);
    }
}
