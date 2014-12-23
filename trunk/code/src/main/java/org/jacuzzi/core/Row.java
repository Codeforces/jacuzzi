package org.jacuzzi.core;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Mike Mirzayanov
 */
public class Row implements Map<String, Object> {
    private static final int MAX_ARRAY_MAP_SIZE = 12;

    private final Map<String, Object> delegateMap;

    public Row(int capacity) {
        if (capacity <= MAX_ARRAY_MAP_SIZE) {
            delegateMap = new ArrayMap<String, Object>(capacity);
        } else {
            delegateMap = new HashMap<String, Object>(capacity, 1.0f);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public Row() {
        delegateMap = new HashMap<String, Object>();
    }

    /**
     * Extracts all rows from the result set and return them as List.
     *
     * @param resultSet JDBC result set to be read.
     * @return List<Row> Rows in result set.
     */
    static List<Row> readFromResultSet(ResultSet resultSet) {
        ArrayList<Row> result = new ArrayList<Row>();

        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                addRowFromResultSet(resultSet, result, metaData);
            }
        } catch (SQLException e) {
            throw new DatabaseException("Can't read the list of rows from the result set.", e);
        } finally {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // No operations.
            }
        }

        result.trimToSize();
        return result;
    }

    /**
     * Extracts the first row from the result set.
     *
     * @param resultSet JDBC result set to be read.
     * @return Row The first row from the result set.
     */
    static Row readFirstFromResultSet(ResultSet resultSet) {
        List<Row> result = new ArrayList<Row>(1);

        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (resultSet.next()) {
                addRowFromResultSet(resultSet, result, metaData);
            }
        } catch (SQLException e) {
            throw new DatabaseException("Can't read the first row from the result set.", e);
        } finally {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
                // No operations.
            }
        }

        return result.size() == 1 ? result.get(0) : null;
    }

    /**
     * Reads the current row from result set and adds it to the list.
     *
     * @param resultSet Result set to be read.
     * @param result    List to be appended by new row.
     * @param metaData  JDBC meta data.
     */
    private static void addRowFromResultSet(ResultSet resultSet, List<Row> result, ResultSetMetaData metaData) {
        try {
            Row row = new Row(metaData.getColumnCount());
            for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            result.add(row);
        } catch (SQLException e) {
            throw new DatabaseException("Can't add row from the result set.", e);
        }
    }

    @Override
    public int size() {
        return delegateMap.size();
    }

    @Override
    public boolean isEmpty() {
        return delegateMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegateMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegateMap.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return delegateMap.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        if (value != null && value.getClass().equals(java.sql.Timestamp.class)) {
            value = new Date(((java.sql.Timestamp) value).getTime());
        }

        if (value != null && value.getClass().equals(java.sql.Date.class)) {
            value = new Date(((java.sql.Date) value).getTime());
        }

        return delegateMap.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return delegateMap.remove(key);
    }

    @Override
    public void putAll(@Nonnull Map<? extends String, ?> m) {
        delegateMap.putAll(m);
    }

    @Override
    public void clear() {
        delegateMap.clear();
    }

    @Override
    @Nonnull
    public Set<String> keySet() {
        return delegateMap.keySet();
    }

    @Override
    @Nonnull
    public Collection<Object> values() {
        return delegateMap.values();
    }

    @Override
    @Nonnull
    public Set<Entry<String, Object>> entrySet() {
        return delegateMap.entrySet();
    }
}
