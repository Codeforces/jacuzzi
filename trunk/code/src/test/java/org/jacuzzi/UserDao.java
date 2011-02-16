package org.jacuzzi;

import java.util.List;

/**
 * @author Mike Mirzayanov
 */
public interface UserDao {
    List<User> findByName(String name);

    void insert(User user);

    void insert(List<User> users);

    void insert(User... users);

    User findOnlyByName(String name);

    List<User> findAll();

    void delete(User user);
}
