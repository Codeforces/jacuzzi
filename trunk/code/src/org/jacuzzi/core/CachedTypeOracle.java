package org.jacuzzi.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mike Mirzayanov
 */
class CachedTypeOracle<T> extends TypeOracle<T> {
    private static ThreadLocal<Map<Class<?>, CachedTypeOracle<?>>> threadLocalCache = new ThreadLocal<Map<Class<?>, CachedTypeOracle<?>>>() {
        protected Map<Class<?>, CachedTypeOracle<?>> initialValue() {
            return new HashMap<Class<?>, CachedTypeOracle<?>>();
        }
    };

    private TypeOracle<T> typeOracle;

    TypeOracle<T> getTypeOracle() {
        return typeOracle;
    }

    CachedTypeOracle(Class<T> typeClass) {
        typeOracle = new TypeOracleImpl<T>(typeClass);
    }

    static Map<Class<?>, CachedTypeOracle<?>> getThreadLocalCache() {
        return threadLocalCache.get();
    }

    public String getIdColumn() {
        return typeOracle.getIdColumn();
    }

    public String getTableName() {
        return typeOracle.getTableName();
    }

    public T convertFromRow(Row row) {
        return typeOracle.convertFromRow(row);
    }

    public String getQuerySetSql() {
        return typeOracle.getQuerySetSql();
    }

    public Object[] getQuerySetArguments(T instance) {
        return typeOracle.getQuerySetArguments(instance);
    }

    public Object getIdValue(T instance) {
        return typeOracle.getIdValue(instance);
    }

    public T newInstance() {
        return typeOracle.newInstance();
    }
}
