package org.jacuzzi.core;

import java.util.List;

/**
 * Provides some usefull methods for entity.
 * <p/>
 * Example usage:
 * <code>
 * public class UserDao extends GenericDaoImpl<User,String> {
 * public UserDao(DataSource source) {
 * super(source);
 * }
 * }
 * </code>
 *
 * @author Mike Mirzayanov
 */
public interface GenericDao<T, K> {
    /**
     * Returns entity by primary key.
     *
     * @param id Primary key value (id).
     * @return Entity instance.
     */
    T find(K id);

    /**
     * Returns all instances of the entity.
     *
     * @return List of all instances of the entity.
     */
    List<T> findAll();

    /**
     * Returns entity list using query to find them.
     * You can use reduced form of the query.
     * Examples:
     * <code>
     * users = userDao.findBy("id > 4");
     * users = userDao.findBy("SELECT * FROM Users WHERE age >= 21");
     * </code>
     *
     * @param query Query (possibly in reduced form).
     * @param args  Arguments to replace "?" jokers in {@code query}.
     * @return The list of selected entities.
     */
    List<T> findBy(String query, Object... args);

    /**
     * Returns the only instance for the specified query.
     *
     * @param throwIfNotUnique if {@code true} then method will throw DatabaseException
     *                         if given query returns more than one row.
     * @param query            SQL-expression returning result set, containing rows for T. It is possible to use
     *                         reduced form (like "login=?").
     * @param args              Arguments to be substituted instead of "?".
     * @return Instance of T or {@code null} if no instance found.
     */
    T findOnlyBy(boolean throwIfNotUnique, String query, Object... args);

    /**
     * Saves (inserts or updates) entity instance.
     *
     * @param object Entity instance.
     */
    void save(T object);

    /**
     * Inserts entity instance. It updates instance's @Id field if
     * insert statement generates exactly one generated key (auto-incremented value).
     *
     * @param object Entity instance.
     * @return {@code true} iff inserted succesfully.
     *         Can return {@code false} if some database restrictions breaked.
     */
    boolean insert(T object);

    /**
     * Updates entity instance.
     *
     * @param object Entity instance.
     * @return {@code true} iff inserted succesfully.
     *         Can return {@code false} if some database restrictions breaked.
     */
    boolean update(T object);

    /**
     * Deletes instance.
     *
     * @param object Object to be deleted.
     * @return {@code true} iff deleted succesfully.
     */
    boolean delete(T object);

    /**
     * Creates new entity instance.
     * Doesn't set any field.
     *
     * @return Created entity instance.
     */
    T newInstance();
}
