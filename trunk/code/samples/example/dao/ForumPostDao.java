package example.dao;

import example.model.ForumPost;
import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/** @author: Mike Mirzayanov */
public class ForumPostDao extends GenericDaoImpl<ForumPost, String> {
    protected ForumPostDao(DataSource source) {
        super(source);
    }
}
