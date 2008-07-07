package org.jacuzzi.core;

import java.util.Map;

/**
 * @author Mike Mirzayanov
 */
public abstract class TypeOracle<T> {
    public abstract String getIdColumn();

    public abstract String getTableName();

    public abstract T convertFromRow(Row row);

    public abstract String getQuerySetSql();

    public abstract Object[] getQuerySetArguments(T instance);

    public abstract Object getIdValue(T instance);

    public abstract T newInstance();

    public static synchronized <T> TypeOracle<T> getTypeOracle(Class<T> typeClass) {
        Map<Class<?>, CachedTypeOracle<?>> cache = CachedTypeOracle.getThreadLocalCache();

        if (cache.containsKey(typeClass)) {
            return (CachedTypeOracle<T>) cache.get(typeClass);
        } else {
            System.out.println("TypeOracle created!");
            TypeOracle<T> typeOracle = new CachedTypeOracle<T>(typeClass);
            cache.put(typeClass, new CachedTypeOracle<T>(typeClass));
            return typeOracle;
        }
    }
}
