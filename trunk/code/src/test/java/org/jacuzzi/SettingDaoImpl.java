package org.jacuzzi;

import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/**
 * @author Maxim Shipko (sladethe@gmail.com)
 *         Date: 23.12.14
 */
public class SettingDaoImpl extends GenericDaoImpl<Setting, Long> implements SettingDao {
    protected SettingDaoImpl(DataSource source) {
        super(source);
    }
}
