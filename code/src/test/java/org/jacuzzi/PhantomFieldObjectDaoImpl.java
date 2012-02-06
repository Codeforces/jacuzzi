package org.jacuzzi;

import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class PhantomFieldObjectDaoImpl extends GenericDaoImpl<PhantomFieldObject, Integer>
        implements PhantomFieldObjectDao {
    protected PhantomFieldObjectDaoImpl(DataSource source) {
        super(source);
    }
}
