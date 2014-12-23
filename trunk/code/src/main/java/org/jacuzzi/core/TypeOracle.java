package org.jacuzzi.core;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Mike Mirzayanov
 */
public abstract class TypeOracle<T> {
    public abstract String getIdColumn();

    public abstract String getTableName();

    public abstract T convertFromRow(Row row);

    public abstract List<T> convertFromRows(List<Row> rows);

    abstract String getFieldList(boolean includeId, boolean useTablePrefix, OperationType operationType);

    abstract String getValuesPatternListForInsert(boolean includeId, T instance);

    abstract List<Object> getValueListForInsert(boolean includeId, T instance);

    abstract boolean hasReasonableId(T instance);

    public abstract String getQuerySetSql();

    /**
     * @param fields Fields to find joined with AND operator.
     * @return WHERE section. For example: "`id`=? AND `name`=?".
     */
    public abstract String getQueryFindSql(String[] fields);

    abstract List<Object> getQuerySetArguments(T instance);

    public abstract void setIdValue(T instance, Object value);

    public abstract Object getIdValue(T instance);

    public abstract T newInstance();

    @SuppressWarnings({"unchecked"})
    public static <T> TypeOracle<T> getTypeOracle(Class<T> typeClass) {
        Map<Class<?>, TypeOracle<?>> cache = CachedTypeOracle.getThreadLocalCache();

        if (cache.containsKey(typeClass)) {
            return (TypeOracle<T>) cache.get(typeClass);
        } else {
            TypeOracle<T> typeOracle = new CachedTypeOracle<T>(typeClass);
            cache.put(typeClass, typeOracle);
            return typeOracle;
        }
    }

    @SuppressWarnings({"unchecked"})
    static <T> T convertTo(Object parameter, Class<T> expectedClazz) {
        if (parameter == null) {
            return null;
        }

        if (parameter.getClass().equals(String.class) && expectedClazz.isEnum()) {
            return convertStringToEnum((String) parameter, expectedClazz);
        }

        if (expectedClazz.equals(String.class) && !parameter.getClass().equals(String.class)) {
            return (T) parameter.toString();
        }

        if (parameter.getClass().equals(java.sql.Timestamp.class) && expectedClazz.equals(java.util.Date.class)) {
            java.sql.Timestamp timestamp = (Timestamp) parameter;
            return (T) new java.util.Date(timestamp.getTime());
        }

        if (parameter.getClass().equals(java.sql.Date.class) && expectedClazz.equals(java.util.Date.class)) {
            java.sql.Date date = (Date) parameter;
            return (T) new java.util.Date(date.getTime());
        }

        return (T) parameter;
    }

    @SuppressWarnings({"unchecked"})
    static <T> T convertStringToEnum(String s, Class<T> expectedClazz) {
        Object[] values = expectedClazz.getEnumConstants();

        for (Object value : values) {
            if (value.toString().equals(s)) {
                return (T) value;
            }
        }

        throw new NoSuchElementException("Can't find element " + s + " in " + expectedClazz + '.');
    }

    public enum OperationType {
        SELECT,
        INSERT,
        UPDATE
    }
}
