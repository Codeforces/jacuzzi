package org.jacuzzi.core;

/**
 * This exception is used instead of SQLException because
 * I believe that database invalid state or behavor is
 * runtime exception.
 *
 * @author: Mike Mirzayanov
 */
public class DatabaseException extends RuntimeException {
    /** @param message Exception message. */
    public DatabaseException(String message) {
        super(message);
    }

    /**
     * @param message Exception message.
     * @param cause   Parent exception or error.
     */
    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    /** @param cause Parent exception or error. */
    public DatabaseException(Throwable cause) {
        super(cause);
    }
}
