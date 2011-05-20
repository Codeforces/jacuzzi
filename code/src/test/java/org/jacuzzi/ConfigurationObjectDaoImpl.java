package org.jacuzzi;

import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class ConfigurationObjectDaoImpl extends GenericDaoImpl<ConfigurationObject, Integer>
        implements ConfigurationObjectDao {
    protected ConfigurationObjectDaoImpl(DataSource source) {
        super(source);
    }
}
