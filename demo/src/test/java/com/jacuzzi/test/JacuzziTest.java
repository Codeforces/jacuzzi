/* Copyright by Mike Mirzayanov. */

package com.jacuzzi.test;

import junit.framework.TestCase;
import com.jacuzzi.test.dao.UserDao;
import com.jacuzzi.test.model.User;

/** @author Mike Mirzayanov */
public class JacuzziTest extends TestCase {
    private UserDao userDao;
    private static final int ITERATION_COUNT = 20;

    protected void setUp() {
        userDao = new UserDao();
        userDao.createStorage();
    }

    protected void tearDown() {
        userDao.deleteStorage();
    }

    public void testInitialization() {
        // No operations.
    }

    public void testInsertAndFind() {
        User user = new User();
        user.setLogin("jacuzzi");
        userDao.insert(user);
        assertEquals(1, userDao.findBy("login=?", "jacuzzi").size());
    }

    public void testManyInsertsAndFinds() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            User user = new User();
            user.setLogin("jacuzzi" + i);
            userDao.insert(user);
            assertEquals(1, userDao.findBy("login=?", "jacuzzi" + i).size());
        }
        for (int i = 0; i < ITERATION_COUNT; i++) {
            User user = new User();
            user.setLogin("jacuzzi");
            userDao.insert(user);
            assertEquals(i + 1, userDao.findBy("login=?", "jacuzzi").size());
        }
    }

    public void testTransaction() {
        userDao.returnJacuzzi().beginTransaction();

        for (int i = 0; i < ITERATION_COUNT; i++) {
            User user = new User();
            user.setLogin("jacuzzi");
            userDao.insert(user);
        }

        userDao.returnJacuzzi().commit();
        assertEquals(ITERATION_COUNT, userDao.findBy("login=?", "jacuzzi").size());

        userDao.returnJacuzzi().beginTransaction();

        for (int i = 0; i < ITERATION_COUNT; i++) {
            User user = new User();
            user.setLogin("jacuzzi");
            userDao.insert(user);
        }

        userDao.returnJacuzzi().rollback();
        assertEquals(ITERATION_COUNT, userDao.findBy("login=?", "jacuzzi").size());
    }
}
