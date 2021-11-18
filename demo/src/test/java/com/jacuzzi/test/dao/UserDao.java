/* Copyright by Mike Mirzayanov. */

package com.jacuzzi.test.dao;

import org.jacuzzi.core.GenericDaoImpl;
import org.jacuzzi.core.Jacuzzi;

import javax.sql.DataSource;

import com.jacuzzi.test.model.User;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/** @author Mike Mirzayanov */
public class UserDao extends GenericDaoImpl<User, Long> {
    public static final String CREATE_USER_TABLE_SQL = "CREATE TABLE  User (" +
            "id BIGINT NOT NULL AUTO_INCREMENT, " +
            "login VARCHAR( 255 ) NOT NULL, " +
            "PRIMARY KEY  (id)" +
            ") ENGINE = InnoDB CHARACTER SET = utf8";

    private static final String DELETE_USER_TABLE_SQL = "DROP TABLE User";

    public UserDao() {
        super(newDataSource());
    }

    private static DataSource newDataSource() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL("jdbc:mysql://localhost:3307/jacuzzi?useUnicode=true&amp;characterSetResults=UTF-8&amp;characterEncoding=UTF-8");
        dataSource.setUser("jacuzzi");
        dataSource.setPassword("jacuzzi");
        return dataSource;
    }

    public void createStorage() {
        getJacuzzi().execute(CREATE_USER_TABLE_SQL);
    }

    public void deleteStorage() {
        getJacuzzi().execute(DELETE_USER_TABLE_SQL);
    }

    public Jacuzzi returnJacuzzi() {
        return getJacuzzi();
    }

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
