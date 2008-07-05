package example.dao;

import example.model.Group;
import example.model.User;
import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;
import java.sql.SQLException;

/** @author: Mike Mirzayanov */
public class GroupDao extends GenericDaoImpl<Group, Long> {
    protected GroupDao(DataSource source) {
        super(source);
    }

    public Group findByUser(User user) {
        return convertFromRow(getJacuzzi().findFirstRow("SELECT `Group`.* " +
                "FROM `Group`, `User` " +
                "WHERE `Group`.id = `User`.groupId AND `User`.id = ?",
                user.getId())
        );
    }
}
