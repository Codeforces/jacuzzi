package org.jacuzzi.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings("unused")
public class RowRoll {
    private long type;
    private String[] keys;
    private int[] keyHashCodes;
    private ArrayList<Object[]> valuesList;

    @SuppressWarnings("WeakerAccess")
    public void setKeys(String[] keys) {
        this.keys = keys;
        initializeKeyHashCodesAndType();
    }

    private long calculateType() {
        long type = 0;
        for (int i = 0; i < keys.length; i++) {
            type = type * 1009 + keys[i].length();
            type = type * 2339 + keyHashCodes[i];
        }
        return type;
    }

    public void addRow(Row row) {
        if (keys == null) {
            keys = new String[row.size()];
            Set<String> rowKeys = row.keySet();
            int index = 0;
            for (String rowKey: rowKeys) {
                keys[index++] = rowKey;
            }

            initializeKeyHashCodesAndType();
        }

        Object[] values = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = row.get(keys[i]);
        }

        if (valuesList == null) {
            valuesList = new ArrayList<>(1);
        }
        valuesList.add(values);
    }

    private void initializeKeyHashCodesAndType() {
        keyHashCodes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyHashCodes[i] = keys[i].hashCode();
        }

        type = calculateType();
    }

    @SuppressWarnings("WeakerAccess")
    public void addValues(Object[] values) {
        if (values.length != keys.length) {
            throw new IllegalArgumentException("Illegal values size: values.length != keys.length [values.length="
                    + values.length + ", keys.length=" + keys.length + "].");
        }

        if (valuesList == null) {
            valuesList = new ArrayList<>(1);
        }
        valuesList.add(values);
    }

    public Row getRow(int index) {
        if (index < 0 || index >= valuesList.size()) {
            throw new IllegalArgumentException("Illegal index for RowRoll object: expected in [0, "
                    + valuesList.size() + ") but " + index + " found.");
        }

        ArrayMap<String, Object> arrayMap = new ArrayMap<>(keys, keyHashCodes, valuesList.get(index));
        return new Row(arrayMap);
    }

    @SuppressWarnings({"WeakerAccess", "BooleanMethodIsAlwaysInverted"})
    public boolean isEmpty() {
        return valuesList == null || valuesList.isEmpty();
    }

    public int size() {
        return valuesList == null ? 0 : valuesList.size();
    }

    public void add(RowRoll rowRoll) {
        if (rowRoll.keys == null) {
            return;
        }

        if (type != rowRoll.type) {
            throw new IllegalArgumentException("This rowRoll and added rowRoll are not compatible.");
        }

        if (rowRoll.valuesList == null || rowRoll.valuesList.isEmpty()) {
            return;
        }

        if (valuesList == null) {
            valuesList = rowRoll.valuesList;
        } else {
            valuesList.addAll(rowRoll.valuesList);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public void trimToSize() {
        if (valuesList != null) {
            valuesList.trimToSize();
        }
    }

    @SuppressWarnings("WeakerAccess")
    public int getColumnCount() {
        return keys == null ? 0 : keys.length;
    }

    String[] getKeys() {
        return keys;
    }

    List<Object[]> getValueList() {
        return valuesList;
    }

    public int getColumn(String key) {
        if (keys == null || key == null) {
            return -1;
        }

        int keyHashCode = key.hashCode();
        for (int i = 0; i < keys.length; i++) {
            if (keyHashCode == keyHashCodes[i] && key.equals(keys[i])) {
                return i;
            }
        }

        return -1;
    }

    public Object getValue(int index, int column) {
        if (column == -1) {
            return null;
        }

        return valuesList.get(index)[column];
    }
}
