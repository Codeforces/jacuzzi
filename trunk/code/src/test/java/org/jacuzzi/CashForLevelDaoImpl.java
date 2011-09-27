package org.jacuzzi;

import org.jacuzzi.core.GenericDaoImpl;

import javax.sql.DataSource;

/**
 * @author Dmitry Levshunov (d.levshunov@drimmi.com)
 */
public class CashForLevelDaoImpl extends GenericDaoImpl<CashForLevel, Long> implements CashForLevelDao {
    // Empty.

    protected CashForLevelDaoImpl(DataSource source) {
        super(source);
    }
}
