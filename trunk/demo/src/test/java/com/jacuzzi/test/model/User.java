/* Copyright by Mike Mirzayanov. */

package com.jacuzzi.test.model;

import org.jacuzzi.mapping.Id;

/** @author Mike Mirzayanov */
public class User {
    @Id
    private long id;
    private String login;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }
}
