package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class is used to manipulate with dataSource. Jacuzzi don't call
 * dataSource.getConnection() explicitly. It uses DataSourceUtil.getConnection(dataSource).
 * <p/>
 * If you are using any pooling dataSource, DataSourceUtil.getConnection(dataSource) can return
 * new connection each call. Use DataSourceUtil.attachConnection() / DataSourceUtil.detachConnection()
 * if you want to attach one connection to the thread. It can be usefull if you want to be sure
 * that several queries are processed with one connection.
 *
 * @author Mike Mirzayanov
 */
class DataSourceUtil {
    /**
     * Attached connection per thread.
     */
    private static final ThreadLocal<Connection> attachedConnection = new ThreadLocal<Connection>();

    /**
     * You can attach connection for a short time (default value is 10 sec).
     * This value stores expiration time.
     */
    private static final ThreadLocal<Long> attachExpirationTime = new ThreadLocal<Long>();

    /**
     * Default lease time.
     */
    private static final long LEASE_TIME = /*10000;*/ Integer.MAX_VALUE;

    /**
     * Attaches connection to the current thread.
     * Starting from the call Jacuzzi will operate with
     * single JDBC connection. Use detachConnection() to
     * detach it.
     *
     * @param dataSource DataSource to get new connection.
     */
    public void attachConnection(DataSource dataSource) {
        if (!isConnectionAttached()) {
            try {
                attachedConnection.set(dataSource.getConnection());
            } catch (SQLException e) {
                throw new DatabaseException("Can't get connection from DataSource instance.", e);
            }
            attachExpirationTime.set(System.currentTimeMillis() + LEASE_TIME);
        }
    }

    private static boolean isConnectionAttached() {
        return attachExpirationTime.get() != null;
    }

    /**
     * Detaches connection from the current thread.
     */
    public void detachConnection() {
        if (!isConnectionAttached()) {
            throw new DatabaseException("detachConnection() called but attached connection not found.");
        }

        long expTime = attachExpirationTime.get();
        if (expTime < System.currentTimeMillis()) {
            throw new DatabaseException("Attached connection expired. You shouldn't do such long attachments.");
        }

        Connection connection = attachedConnection.get();

        attachExpirationTime.remove();
        attachedConnection.remove();

        closeConnection(connection);
    }

    /**
     * @param dataSource     of type DataSource
     * @param expectAttached <code>true</code> iff DatabaseException should be thrown on no attached connection.
     * @return Connection instance.
     */
    private static Connection getConnection(DataSource dataSource, boolean expectAttached) {
        try {
            if (isConnectionAttached()) {
                long expTime = attachExpirationTime.get();
                if (System.currentTimeMillis() > expTime) {
                    throw new DatabaseException("Attached connection expired. You shouldn't do such long attachments.");
                }

                return attachedConnection.get();
            } else {
                if (expectAttached) {
                    throw new DatabaseException("Connection not attached.");
                } else {
                    return dataSource.getConnection();
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Can't get connection from DataSource instance.", e);
        }
    }

    /**
     * @param dataSource of type DataSource
     * @return Connection Finds new connection. If connection has been attached, then it returns one connection
     *         each call before detachConnection().
     */
    public Connection getConnection(DataSource dataSource) {
        return getConnection(dataSource, false);
    }

    /**
     * @param dataSource of type DataSource
     * @return Connection Returns attached connection or throws DatabaseException if it doesn't attached.
     */
    public Connection getAttachedConnection(DataSource dataSource) {
        return getConnection(dataSource, true);
    }

    /**
     * Use it to close connection safely. Don't use connection.close().
     *
     * @param connection of type Connection
     */
    public void closeConnection(Connection connection) {
        if (!isConnectionAttached()) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new DatabaseException("Can't close connection.", e);
            }
        }
    }
}
