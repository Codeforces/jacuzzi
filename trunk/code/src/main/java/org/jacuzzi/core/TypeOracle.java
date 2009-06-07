package org.jacuzzi.core;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Mike Mirzayanov
 */
public abstract class TypeOracle<T> {
    public abstract String getIdColumn();

    public abstract String getTableName();

    public abstract T convertFromRow(Row row);

    public abstract String getQuerySetSql();

    public abstract Object[] getQuerySetArguments(T instance);

    public abstract void setIdValue(T instance, Object value);

    public abstract Object getIdValue(T instance);

    public abstract T newInstance();

    public static synchronized <T> TypeOracle<T> getTypeOracle(Class<T> typeClass) {
        Map<Class<?>, CachedTypeOracle<?>> cache = CachedTypeOracle.getThreadLocalCache();

        if (cache.containsKey(typeClass)) {
            return (CachedTypeOracle<T>) cache.get(typeClass);
        } else {
            //System.out.println("TypeOracle created!");
            CachedTypeOracle<T> typeOracle = new CachedTypeOracle<T>(typeClass);
            cache.put(typeClass, typeOracle);
            return typeOracle;
        }
    }

    static<T> T convertTo(Object parameter, Class<T> expectedClazz) {
        if (parameter == null) {
            return null;
        } else {
            if (parameter.getClass().equals(String.class) && expectedClazz.isEnum()) {
                return convertStringToEnum((String) parameter, expectedClazz);
            } else {
                return (T) parameter;
            }
        }
    }

    static<T> T convertStringToEnum(String s, Class<T> expectedClazz) {
        Object[] values = expectedClazz.getEnumConstants();

        for (Object value : values) {
            if (value.toString().equals(s)) {
                return (T) value;
            }
        }

        throw new NoSuchElementException("Can't find element " + s + " in " + expectedClazz + ".");
    }

}
