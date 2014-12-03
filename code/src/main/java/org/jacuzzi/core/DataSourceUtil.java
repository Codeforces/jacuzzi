package org.jacuzzi.core;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final ThreadLocal<Map<DataSource, Attachment>> attachments = new ThreadLocal<Map<DataSource, Attachment>>() {
        @Override
        protected Map<DataSource, Attachment> initialValue() {
            return new ArrayMap<DataSource, Attachment>(4);
        }
    };

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
        final Attachment attachment = attachments.get().get(dataSource);

        if (attachment == null) {
            try {
                attachments.get().put(dataSource, new Attachment(dataSource.getConnection(), System.currentTimeMillis() + LEASE_TIME));
            } catch (SQLException e) {
                throw new DatabaseException("Can't get connection from DataSource instance.", e);
            }
        } else {
            attachment.incrementAttachCount();
        }
    }

    /**
     * Detaches connection from the current thread.
     */
    public void detachConnection(final DataSource dataSource) {
        internalDetachConnection(dataSource, false);
    }

    public void detachConnectionOrThrowException(DataSource dataSource) {
        internalDetachConnection(dataSource, true);
    }

    private void internalDetachConnection(DataSource dataSource, boolean throwOnNonClose) {
        final Attachment attachment = attachments.get().get(dataSource);

        if (attachment == null) {
            throw new DatabaseException("detachConnection() called but attached connection not found.");
        }

        if (attachment.expirationTime < System.currentTimeMillis()) {
            throw new DatabaseException("Attached connection expired. You shouldn't do such long attachments.");
        }

        final int attachCount = attachment.decrementAttachCount();
        if (attachCount == 0) {
            attachments.get().remove(dataSource);
            closeConnection(dataSource, attachment.connection);
        } else {
            if (throwOnNonClose) {
                throw new DatabaseException("Expected connection to be completely detached, but attachCount=" + attachCount + ".");
            }
        }
    }

    /**
     * @param dataSource     of type DataSource
     * @param expectAttached <code>true</code> iff DatabaseException should be thrown on no attached connection.
     * @return Connection instance.
     */
    private static Connection getConnection(DataSource dataSource, boolean expectAttached) {
        try {
            final Attachment attachment = attachments.get().get(dataSource);

            if (attachment != null) {
                if (attachment.expirationTime < System.currentTimeMillis()) {
                    throw new DatabaseException("Attached connection expired. You shouldn't do such long attachments.");
                }
                return attachment.connection;
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
    public void closeConnection(DataSource dataSource, Connection connection) {
        if (!attachments.get().containsKey(dataSource)) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new DatabaseException("Can't close connection.", e);
            }
        }
    }

    private static final class Attachment {
        private final Connection connection;
        private final long expirationTime;
        private final AtomicInteger attachCount = new AtomicInteger();

        private Attachment(Connection connection, long expirationTime) {
            this.connection = connection;
            this.expirationTime = expirationTime;

            if (this.attachCount.incrementAndGet() != 1) {
                throw new RuntimeException("Expected attachCount=1.");
            }
        }

        private int incrementAttachCount() {
            return attachCount.incrementAndGet();
        }

        private int decrementAttachCount() {
            return attachCount.decrementAndGet();
        }
    }
}
