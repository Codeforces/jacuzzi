package example;

import example.dao.EventDao;
import example.dao.GroupDao;
import example.dao.UserDao;
import example.dao.Event2Dao;
import example.model.Group;
import example.model.User;
import example.model.Event;
import example.model.Event2;
import org.jacuzzi.core.Jacuzzi;

import javax.sql.DataSource;
import java.util.List;
import java.util.Date;

/** @author: Mike Mirzayanov */
public class Test {
    public static void main(DataSource source) {
        Event2Dao eventDao = new Event2Dao(source);

        Event2 event = new Event2();
        event.setTitle("This is sample:)");
        event.setCreationTime(new Date());

        eventDao.save(event);
    }

    public static void main2(DataSource source) {
        Jacuzzi jacuzzi = Jacuzzi.getJacuzzi(source);
        UserDao userDao = jacuzzi.getDao(UserDao.class);
        EventDao eventDao = jacuzzi.getDao(EventDao.class);
        GroupDao groupDao = jacuzzi.getDao(GroupDao.class);

        System.out.println("=== 1 ===");

        User mike = userDao.find(1L);
        List<User> mikeFriends = userDao.findFriends(mike);

        showUserAndFriends(mike, mikeFriends);

        System.out.println("=== 2 ===");

        Group csit = groupDao.find(511L);
        System.out.println(csit.getName() + " " + csit.getDescription());

        Group mikeGroup = userDao.findGroup(mike);
        System.out.println(mikeGroup.getName() + " " + mikeGroup.getDescription());

        System.out.println("=== 3 ===");

        User user = userDao.findByEvent(eventDao.find(1L));
        System.out.println(user.getName());
    }

    private static void showUserAndFriends(User user, List<User> friends) {
        System.out.println(user.getName());
        for (User friend: friends) {
            System.out.println("> " + friend.getName());
        }
    }
}
