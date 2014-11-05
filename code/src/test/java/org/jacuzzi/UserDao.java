package org.jacuzzi;

import java.util.List;

/**
 * @author Mike Mirzayanov
 */
public interface UserDao {
    long findCountBy(String query, Object... args);

    List<User> findByName(String name);

    void insert(User user);

    void insert(List<User> users);

    void insert(User... users);

    User findOnlyByName(String name);

    List<User> findAll();

    List<User> findBy(String query, Object... args);

    void delete(User user);
}
