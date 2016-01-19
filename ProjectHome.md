Jacuzzi works around java.sql.Connection and it is all you need to provide. Jacuzzi don't use any configuration files and uses just a few annotations. It doesn't take care about abstract cross-database query language, use native SQL to write queries or methods from GenericDaoImpl to interact with database.

# Usage #
Domain class:
```
public class User {
    @Id
    private long id;

    private String name;

    private String login;

    // ...
    // Getters and setters are here
    // ...
```

Interface:
```
public interface UserDao {
    List<User> findByName(String name);
    void insert(User user);
    User findByLogin(String login);
    User find(long id);
    void delete(User user);
}
```

Implementation:
```
public class UserDaoImpl extends GenericDaoImpl<User, Long> implements UserDao {
    public UserDaoImpl(DataSource source) {
        super(source);
    }

    @Override
    public List<User> findByName(String name) {
        return findBy("name=?", name);
    }

    @Override
    public User findByLogin(String login) {
        return findOnlyBy(true, "login=?", login);
    }

    @Override
    public User find(long id) {
        return super.find(id);
    }
}
```

# How to setup #
Use the following maven 2 settings to setup jacuzzi dependency:
```
  <repositories>
      <repository>
          <id>jacuzzi</id>
          <url>http://jacuzzi.googlecode.com/svn/trunk/repo/</url>
      </repository>
  </repositories>
    ...
  <dependencies>
        ...
    <dependency>
        <groupId>jacuzzi</groupId>
        <artifactId>jacuzzi</artifactId>
        <version>0.0.27-SNAPSHOT</version>
    </dependency>
        ...
  </dependencies>
```