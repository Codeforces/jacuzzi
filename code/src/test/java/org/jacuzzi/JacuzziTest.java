package org.jacuzzi;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import junit.framework.TestCase;
import org.jacuzzi.core.DatabaseException;
import org.jacuzzi.core.GenericDaoImpl;
import org.jacuzzi.core.Jacuzzi;
import org.jacuzzi.core.Row;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author Mike Mirzayanov
 */
public class JacuzziTest extends TestCase {
    private UserDao userDao;
    private CommonDao commonDao;
    private DataSource dataSource;

    private static DataSource newDataSource() {
        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl("jdbc:hsqldb:mem:jacuzzi");
        comboPooledDataSource.setUser("sa");
        comboPooledDataSource.setPassword("");
        return comboPooledDataSource;
    }

    @Override
    protected void setUp() throws Exception {
        dataSource = newDataSource();
        Connection connection = dataSource.getConnection();
        connection.createStatement().execute("DROP TABLE User IF EXISTS");
        connection.createStatement().execute("CREATE TABLE User (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1), name VARCHAR(255), surname VARCHAR(255) NULL)");
        userDao = new UserDaoImpl(dataSource);
        commonDao = new CommonDao(dataSource);
    }

    public void testThatEmptyTableDoesntContainUsersWithSpecificName() throws SQLException {
        assertEquals(0, userDao.findByName("test").size());
    }

    public void testThatEmptyTableDoesntContainUserWithSpecificName() throws SQLException {
        assertNull(userDao.findOnlyByName("test"));
    }

    public void testThatThatUserListContainsExactlyOneItemAfterOneInsert() throws SQLException {
        User user = new User();
        user.setName("jacuzzi");
        userDao.insert(user);
        List<User> users = userDao.findByName("jacuzzi");
        assertEquals(1, users.size());
        assertEquals(1L, users.get(0).getId());
        assertEquals("jacuzzi", users.get(0).getName());
    }

    public void testThatThatManyInsertsWork() throws SQLException {
        int n = 1000;
        for (int i = 0; i < n; i++) {
            User user = new User();
            user.setName("jacuzzi");
            userDao.insert(user);
        }
        List<User> users = userDao.findByName("jacuzzi");
        assertEquals(n, users.size());

        List<Long> ids = new ArrayList<Long>();
        for (User user : users) {
            assertEquals("jacuzzi", user.getName());
            ids.add(user.getId());
        }

        Collections.sort(ids);

        long index = 0;
        for (Long id : ids) {
            index++;
            assertEquals(index, id.longValue());
        }
    }

    public void testThatNullOrEmptyListForMultipleInsertsInOneQueryWork() throws SQLException {
        userDao.insert();

        User[] users = new User[0];
        userDao.insert(users);

        List<User> list = new ArrayList<User>();
        userDao.insert(list);
    }

    public void testThatThatMultipleInsertsInOneQueryWork() throws SQLException {
        User user1 = new User();
        User user2 = new User();
        User user3 = new User();
        User user4 = new User();

        user1.setName("1");
        user2.setName("2");
        user2.setSurname("jacuzzi");
        user3.setName("3");
        user3.setId(5);
        user4.setName("4");

        userDao.insert(user1, user2, user3, user4);

        List<User> users = userDao.findAll();
        assertEquals(4, users.size());

        assertEquals(1, user1.getId());
        assertEquals(2, user2.getId());
        assertEquals(5, user3.getId());
        assertEquals(6, user4.getId());

        assertEquals(1, users.get(0).getId());
        assertEquals(2, users.get(1).getId());
        assertEquals(5, users.get(2).getId());
        assertEquals(6, users.get(3).getId());

        assertEquals("1", users.get(0).getName());
        assertNull(users.get(0).getSurname());

        assertEquals("2", users.get(1).getName());
        assertEquals("jacuzzi", users.get(1).getSurname());

        assertEquals("3", users.get(2).getName());
        assertNull(users.get(2).getSurname());

        assertEquals("4", users.get(3).getName());
        assertNull(users.get(3).getSurname());
    }

    public void testThatThatManyInsertsInOneQueryWork() throws SQLException {
        int n = 1000;

        List<User> usersToInsert = new ArrayList<User>(n);
        for (int i = 0; i < n; i++) {
            User user = new User();
            user.setName("jacuzzi");
            usersToInsert.add(user);
        }

        userDao.insert(usersToInsert);

        List<User> users = userDao.findByName("jacuzzi");
        assertEquals(n, users.size());

        List<Long> ids = new ArrayList<Long>();
        for (User user : users) {
            assertEquals("jacuzzi", user.getName());
            ids.add(user.getId());
        }

        Collections.sort(ids);

        long index = 0;
        for (Long id : ids) {
            index++;
            assertEquals(index, id.longValue());
        }
    }

    public void testPairOfInsertAndRemove() {
        User user = new User();
        user.setId(123);
        user.setName("123");

        commonDao.insert(user);
        assertEquals(1, commonDao.findAll().size());
        assertEquals(123L, commonDao.findAll().get(0).getId());
        commonDao.delete(user);
        assertEquals(0, commonDao.findAll().size());
    }

    public void testUpdate() {
        User user = new User();
        user.setId(123);
        user.setName("test");
        commonDao.insert(user);

        user.setId(1123);
        try {
            commonDao.update(user);
            assertFalse(true);
        } catch (DatabaseException e) {
            // No operations.
        }

        user.setId(123);
        user.setName("tezt");
        commonDao.update(user);
        assertEquals(1, commonDao.findAll().size());
        assertEquals(123L, commonDao.findAll().get(0).getId());
        assertEquals("tezt", commonDao.findAll().get(0).getName());
    }

    public void testJacuzziExecute() {
        User user = new User();
        user.setId(123);
        user.setName("test");
        commonDao.insert(user);

        Jacuzzi jacuzzi = Jacuzzi.getJacuzzi(dataSource);

        assertEquals("test", jacuzzi.findOne("SELECT name FROM User"));
        assertEquals(2, jacuzzi.findFirstRow("SELECT id, name FROM User").size());
        assertEquals(123L, jacuzzi.findLong("SELECT id FROM User"));
        assertEquals(true, jacuzzi.findBoolean("SELECT id=123 FROM User"));
        assertEquals(false, jacuzzi.findBoolean("SELECT id=122 FROM User"));
        assertEquals("test", jacuzzi.findString("SELECT name FROM User"));
        assertEquals(new Date().getTime() / 10000,
                jacuzzi.findDate("SELECT CURRENT_TIMESTAMP FROM User").getTime() / 10000);

        assertEquals(1L, jacuzzi.execute("DELETE FROM User"));
        assertEquals(0L, jacuzzi.execute("DELETE FROM User"));

        for (int i = 0; i < 1234; i++) {
            user = new User();
            user.setId(i);
            user.setName("test");
            userDao.insert(user);
        }
        assertEquals("test", jacuzzi.findOne("SELECT name FROM User WHERE id=789"));
        List<Row> rows = jacuzzi.findRows("SELECT id, name FROM User");
        assertEquals(1234, rows.size());

        assertEquals(1234L, jacuzzi.execute("DELETE FROM User"));
        assertEquals(0L, jacuzzi.execute("DELETE FROM User"));

        assertEquals(Thread.State.BLOCKED, jacuzzi.convertTo("BLOCKED", Thread.State.class));
        assertEquals("BLOCKED", jacuzzi.convertTo(Thread.State.BLOCKED, String.class));
        assertEquals("123", jacuzzi.convertTo(123L, String.class));
    }

    public void testFindOnlyBy() {
        User user = new User();
        user.setId(123);
        user.setName("test");
        commonDao.insert(user);

        assertEquals(123L, userDao.findOnlyByName("test").getId());

        user = new User();
        user.setId(124);
        user.setName("test");
        commonDao.insert(user);

        try {
            userDao.findOnlyByName("test");
            assertFalse(true);
        } catch (DatabaseException e) {
            // No operations.
        }

        user = commonDao.findOnlyBy(false, "name=?", "test");
        assertTrue(user.getId() == 123 || user.getId() == 124);
        assertEquals("test", user.getName());
    }

    public void testTransactions() throws InterruptedException {
        final Jacuzzi jacuzzi = Jacuzzi.getJacuzzi(dataSource);
        User user;

        jacuzzi.beginTransaction();
        user = new User();
        user.setId(123);
        user.setName("test");
        commonDao.insert(user);
        assertEquals(1, commonDao.findAll().size());
        jacuzzi.rollback();
        assertEquals(0, commonDao.findAll().size());

        jacuzzi.beginTransaction();
        user = new User();
        user.setId(123);
        user.setName("test");
        commonDao.insert(user);
        assertEquals(1, commonDao.findAll().size());
        jacuzzi.commit();
        assertEquals(1, commonDao.findAll().size());
        commonDao.delete(user);

        int n = 100;

        jacuzzi.beginTransaction();
        for (int i = 0; i < n; i++) {
            user = new User();
            user.setName("test");
            assertEquals(i, commonDao.findAll().size());
            commonDao.insert(user);
        }
        assertEquals(n, commonDao.findAll().size());
        jacuzzi.rollback();
        assertEquals(0, commonDao.findAll().size());

        jacuzzi.beginTransaction();
        for (int i = 0; i < n; i++) {
            user = new User();
            user.setName("test");
            assertEquals(i, commonDao.findAll().size());
            commonDao.insert(user);
        }
        assertEquals(n, commonDao.findAll().size());
        jacuzzi.commit();
        assertEquals(n, commonDao.findAll().size());
        assertEquals(n, jacuzzi.execute("DELETE FROM User"));

        final int m = 1000;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    jacuzzi.beginTransaction();
                    for (int i = 0; i < m; i++) {
                        User user = new User();
                        user.setName("test");
                        commonDao.insert(user);
                    }
                    jacuzzi.rollback();
                    assertEquals(0, commonDao.findAll().size());
                }
            };

            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(0, commonDao.findAll().size());

        threads.clear();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    jacuzzi.beginTransaction();
                    for (int i = 0; i < m; i++) {
                        User user = new User();
                        user.setName("test");
                        commonDao.insert(user);
                    }
                    jacuzzi.commit();
                }
            };

            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(m * 5, commonDao.findAll().size());
    }

    static {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static class CommonDao extends GenericDaoImpl<User, Long> {
        protected CommonDao(DataSource source) {
            super(source);
        }
    }
}
