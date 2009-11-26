package org.jacuzzi;

import java.util.List;

/** @author Mike Mirzayanov */
public interface UserDao {
    List<User> findByName(String name);
    boolean insert(User user);
    User findOnlyByName(String name);
    boolean delete(User user);
}
