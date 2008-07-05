package example.dao;

import example.model.Event;
import example.model.User;
import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/** @author: Mike Mirzayanov */
public class EventDao extends GenericDaoImpl<Event, Long> {
    protected EventDao(DataSource source) {
        super(source);
    }

    public User getUser(Event event) {
        return getJacuzzi().getDao(UserDao.class).findByEvent(event);
    }
}
