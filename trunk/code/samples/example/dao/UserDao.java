package example.dao;

import example.model.Group;
import example.model.User;
import example.model.Event;
import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

/** @author: Mike Mirzayanov */
public class UserDao extends GenericDaoImpl<User, Long> {
    protected UserDao(DataSource source) {
        super(source);
    }

    public Group findGroup(User user) {
        return getJacuzzi().getDao(GroupDao.class).findByUser(user);
    }

    public List<User> findFriends(User user) {
        long id = user.getId();
        return convertFromRows(
                getJacuzzi().findRows("SELECT User.* FROM User, Friend " +
                        "WHERE " +
                        "(User.id = Friend.second AND Friend.first = ?) " +
                        "OR " +
                        "(User.id = Friend.first AND Friend.second = ?) " +
                        "GROUP BY User.id",
                        id, id
                )
        );
    }

    public User findByEvent(Event event) {
        return convertFromRow(getJacuzzi().findFirstRow("SELECT User.* " +
                "FROM User, Event " +
                "WHERE User.id = Event.userId AND Event.id = ?",
                event.getId())
        );
    }
}
