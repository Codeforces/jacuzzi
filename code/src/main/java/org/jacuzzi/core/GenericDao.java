package org.jacuzzi.core;

import java.util.Collection;
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
     * @param throwIfNotUnique if {@code true} then method will throw {@code {@link DatabaseException}}
     *                         if given query returns more than one row.
     * @param query            SQL-expression returning result set, containing rows for T. It is possible to use
     *                         reduced form (like "login=?").
     * @param args             Arguments to be substituted instead of "?".
     * @return Instance of T or {@code null} if no instance found.
     */
    @SuppressWarnings("OverloadedVarargsMethod")
    T findOnlyBy(boolean throwIfNotUnique, String query, Object... args);

    /**
     * Returns the only instance for the specified query.
     * Method will throw {@code {@link DatabaseException}} if given query returns more than one row.
     *
     * @param query SQL-expression returning result set, containing rows for T. It is possible to use
     *              reduced form (like "login=?").
     * @param args  Arguments to be substituted instead of "?".
     * @return Instance of T or {@code null} if no instance found.
     */
    @SuppressWarnings("OverloadedVarargsMethod")
    T findOnlyBy(String query, Object... args);

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
     * @throws DatabaseException if can't save instance.
     */
    void insert(T object);

    /**
     * Inserts multiple entity instances. It updates instance's @Id field if
     * insert statement generates exactly one generated key (auto-incremented value).
     *
     * @param objects Variable number of entity instances.
     * @throws DatabaseException if can't save instances.
     */
    void insert(T... objects);

    /**
     * Inserts multiple entity instances. It updates instance's @Id field if
     * insert statement generates exactly one generated key (auto-incremented value).
     *
     * @param objects List of entity instances.
     * @throws DatabaseException if can't save instances.
     */
    void insert(List<T> objects);

    /**
     * Updates entity instance.
     *
     * @param object Entity instance.
     * @throws DatabaseException if no such instance found or on many other database errors.
     */
    void update(T object);

    /**
     * Deletes instance.
     *
     * @param object Object to be deleted.
     * @throws DatabaseException if no such instance found or on many other database errors.
     */
    void delete(T object);

    /**
     * Delete multiple instances.
     *
     * @param objects Variable number of entity instances.
     * @throws DatabaseException if can't delete instances.
     */
    void delete(T... objects);

    /**
     * Delete multiple instances.
     *
     * @param objects Collection of entity instances.
     * @throws DatabaseException if can't delete instances.
     */
    void delete(Collection<T> objects);

    /**
     * Deletes instance.
     *
     * @param id Id of object to be deleted.
     * @throws DatabaseException if no such instance found or on many other database errors.
     */
    void deleteById(K id);

    /**
     * Delete multiple instances by id.
     *
     * @param ids Variable number of entity ids.
     * @throws DatabaseException if can't delete instances.
     */
    void deleteById(K... ids);

    /**
     * Delete multiple instances by id.
     *
     * @param ids Collection of entity ids.
     * @throws DatabaseException if can't delete instances.
     */
    void deleteById(Collection<K> ids);

    /**
     * Creates new entity instance.
     * Doesn't set any field.
     *
     * @return Created entity instance.
     */
    T newInstance();
}
